import requests
import pandas as pd
from datetime import datetime
import time
import threading
import os
from pathlib import Path

class WeatherDataExtractor:
    def __init__(self):
        self.API_KEY = "ME1iY94cyCsjtGgJ7eVxGG0eRCGQGdiY"
        self.LOCATION_KEY = "45881"  # São Paulo
        self.FILENAME = "precipitacao_accuweather_sp_hora.xlsx"
        self.running = False
        
    def fetch_weather_data(self):
        """Busca dados da API do AccuWeather"""
        url = f"http://dataservice.accuweather.com/forecasts/v1/hourly/1hour/{self.LOCATION_KEY}"
        params = {
            'apikey': self.API_KEY,
            'language': 'en-us',
            'details': 'true',
            'metric': 'true'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"[{datetime.now():%H:%M:%S}] Erro API: {response.status_code}")
                return None
        except Exception as e:
            print(f"[{datetime.now():%H:%M:%S}] Erro conexão: {e}")
            return None
    
    def process_data(self, data):
        """Processa dados focados em precipitação com formato customizado"""
        if not data:
            return []
        
        processed = []
        for item in data:
            # Extrai data/hora do formato ISO
            datetime_str = item.get('DateTime', '')
            if datetime_str:
                try:
                    dt = pd.to_datetime(datetime_str)
                    data_formatada = dt.strftime('%Y-%m-%d')
                    hora_formatada = dt.strftime('%H:%M:%S')
                except:
                    data_formatada = ''
                    hora_formatada = ''
            else:
                data_formatada = ''
                hora_formatada = ''
            
            # Extrai precipitação (usando TotalLiquid como principal)
            total_liquid = item.get('TotalLiquid', {}).get('Value', 0) if item.get('TotalLiquid') else 0
            
            # Cria registro com as colunas especificadas na ordem exata
            record = {
                'data': data_formatada,
                'hora': hora_formatada,
                'precipitacao_mm': round(total_liquid, 2),
                'umidade_percent': item.get('RelativeHumidity', 0),
                'nuvens_percent': item.get('CloudCover', 0),
                'condicao_tempo': item.get('IconPhrase', ''),
                'chance_chuva': item.get('PrecipitationProbability', 0),
                'HasPrecipitation': item.get('HasPrecipitation', False)
            }
            processed.append(record)
        
        return processed
    
    def save_to_excel(self, new_data):
        """Salva dados no Excel, evitando duplicatas"""
        if not new_data:
            return False
        
        df_new = pd.DataFrame(new_data)
        
        # Cria coluna DateTime temporária para comparação de duplicatas
        df_new['DateTime_temp'] = pd.to_datetime(df_new['data'] + ' ' + df_new['hora'])
        
        # Verifica se arquivo existe e carrega dados existentes
        if os.path.exists(self.FILENAME):
            try:
                df_existing = pd.read_excel(self.FILENAME, sheet_name='Dados_Completos')
                
                # Cria DateTime temporário para dados existentes
                df_existing['DateTime_temp'] = pd.to_datetime(df_existing['data'] + ' ' + df_existing['hora'], errors='coerce')
                
                # Filtra apenas dados novos
                mask = ~df_new['DateTime_temp'].isin(df_existing['DateTime_temp'])
                df_new_filtered = df_new[mask]
                
                if df_new_filtered.empty:
                    print(f"[{datetime.now():%H:%M:%S}] Sem dados novos")
                    return False
                
                # Combina dados
                df_combined = pd.concat([df_existing, df_new_filtered], ignore_index=True)
                df_combined = df_combined.sort_values('DateTime_temp')
            except Exception as e:
                print(f"[{datetime.now():%H:%M:%S}] Erro ao ler arquivo existente: {e}")
                df_combined = df_new
        else:
            df_combined = df_new
        
        # Remove coluna temporária antes de salvar
        df_combined = df_combined.drop('DateTime_temp', axis=1)
        
        # Garante que as colunas estejam na ordem correta
        column_order = ['data', 'hora', 'precipitacao_mm', 'umidade_percent', 'nuvens_percent', 'condicao_tempo', 'chance_chuva', 'HasPrecipitation']
        df_combined = df_combined[column_order]
        
        # Salva em Excel com múltiplas abas
        try:
            with pd.ExcelWriter(self.FILENAME, engine='openpyxl') as writer:
                # Aba principal com todas as colunas especificadas
                df_combined.to_excel(writer, sheet_name='Dados_Completos', index=False)
                
                # Aba com precipitação
                df_precip = df_combined[df_combined['HasPrecipitation'] == True]
                if not df_precip.empty:
                    df_precip.to_excel(writer, sheet_name='Com_Precipitacao', index=False)
                
                # Aba resumo estatístico
                stats = {
                    'Total_Registros': len(df_combined),
                    'Registros_Com_Chuva': df_combined['HasPrecipitation'].sum(),
                    'Precipitacao_Media_mm': df_combined['precipitacao_mm'].mean(),
                    'Precipitacao_Max_mm': df_combined['precipitacao_mm'].max(),
                    'Umidade_Media': df_combined['umidade_percent'].mean(),
                    'Chance_Chuva_Media': df_combined['chance_chuva'].mean(),
                    'Ultima_Atualizacao': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                pd.DataFrame([stats]).T.to_excel(writer, sheet_name='Estatisticas')
            
            print(f"[{datetime.now():%H:%M:%S}] ✓ Dados salvos: {len(df_combined)} registros totais")
            return True
            
        except Exception as e:
            print(f"[{datetime.now():%H:%M:%S}] Erro ao salvar: {e}")
            return False
    
    def collect_data(self):
        """Coleta e processa dados uma vez"""
        print(f"[{datetime.now():%H:%M:%S}] Coletando dados...")
        
        raw_data = self.fetch_weather_data()
        if raw_data:
            processed_data = self.process_data(raw_data)
            if processed_data:
                self.save_to_excel(processed_data)
                return True
        return False
    
    def run_automated(self, hours=24):
        """Executa coleta automaticamente por X horas"""
        self.running = True
        end_time = time.time() + (hours * 3600)
        
        print(f"\n{'='*50}")
        print(f"MODO AUTOMÁTICO INICIADO - {hours} horas")
        print(f"Arquivo: {self.FILENAME}")
        print(f"Coleta a cada hora")
        print(f"Colunas: data | hora | precipitacao_mm | umidade_percent | nuvens_percent | condicao_tempo | chance_chuva | HasPrecipitation")
        print(f"{'='*50}\n")
        
        while self.running and time.time() < end_time:
            self.collect_data()
            
            # Calcula tempo restante
            remaining = int((end_time - time.time()) / 3600)
            if remaining > 0:
                print(f"[{datetime.now():%H:%M:%S}] Próxima coleta em 1 hora ({remaining}h restantes)")
                time.sleep(3600)  # Espera 1 hora
            else:
                break
        
        print(f"\n[{datetime.now():%H:%M:%S}] Coleta automática finalizada!")
        self.running = False
    
    def stop(self):
        """Para a execução automática"""
        self.running = False
        print(f"\n[{datetime.now():%H:%M:%S}] Parando coleta automática...")


def main():
    """Função principal com menu de opções"""
    extractor = WeatherDataExtractor()
    
    print("\n" + "="*50)
    print("EXTRATOR DE DADOS METEOROLÓGICOS - SÃO PAULO")
    print("="*50)
    print("\nFormato das colunas:")
    print("data | hora | precipitacao_mm | umidade_percent | nuvens_percent | condicao_tempo | chance_chuva | HasPrecipitation")
    print("\nOpções:")
    print("1. Coleta única (executar uma vez)")
    print("2. Modo automático 24h (coleta a cada hora)")
    print("3. Modo automático personalizado")
    print("4. Modo contínuo (executar indefinidamente)")
    
    choice = input("\nEscolha uma opção (1-4): ").strip()
    
    if choice == "1":
        # Coleta única
        extractor.collect_data()
        
    elif choice == "2":
        # Automático 24 horas
        extractor.run_automated(24)
        
    elif choice == "3":
        # Automático personalizado
        try:
            hours = int(input("Quantas horas de execução? "))
            if hours > 0:
                extractor.run_automated(hours)
            else:
                print("Número de horas deve ser positivo!")
        except ValueError:
            print("Valor inválido!")
            
    elif choice == "4":
        # Modo contínuo
        print("\n[MODO CONTÍNUO] Pressione Ctrl+C para parar\n")
        try:
            while True:
                extractor.collect_data()
                print(f"[{datetime.now():%H:%M:%S}] Aguardando 1 hora...")
                time.sleep(3600)
        except KeyboardInterrupt:
            print("\n\nColeta interrompida pelo usuário")
            
    else:
        print("Opção inválido!")
    
    print("\n" + "="*50)
    print("EXECUÇÃO FINALIZADA")
    print(f"Arquivo de dados: {extractor.FILENAME}")
    print("="*50 + "\n")


# MODO BACKGROUND (ALTERNATIVO)
def run_background(hours=24):
    """
    Executa em background sem interação
    Útil para deixar rodando em servidor ou computador ligado
    """
    extractor = WeatherDataExtractor()
    
    def background_task():
        extractor.run_automated(hours)
    
    thread = threading.Thread(target=background_task, daemon=True)
    thread.start()
    
    print(f"Coleta automática iniciada em background por {hours} horas")
    print("O script continuará rodando. Pressione Ctrl+C para parar.")
    
    try:
        while thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        extractor.stop()
        print("\nParando coleta...")
        thread.join(timeout=5)


if __name__ == "__main__":
    # Para executar com menu interativo:
    main()
    
    # Para executar diretamente em background por 24 horas, descomente:
    # run_background(24)