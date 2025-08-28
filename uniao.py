import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import threading
import os
from pathlib import Path

class PrecipitationDataExtractor:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "http://api.weatherapi.com/v1"
        self.location = "São Paulo, Brazil"
        self.filename = "precipitacao_sao_paulo.xlsx"
        
    def get_current_precipitation(self):
        """
        Returns:
            dict: Dados de precipitação ou None se houver erro
        """
        url = f"{self.base_url}/current.json"
        params = {
            'key': self.api_key,
            'q': self.location
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao obter dados atuais: {e}")
            return None
    
    def get_forecast_precipitation(self, days=7):
        """
        Args:
            days (int): Número de dias de previsão (1-10)
            
        Returns:
            dict: Dados de previsão ou None se houver erro
        """
        url = f"{self.base_url}/forecast.json"
        params = {
            'key': self.api_key,
            'q': self.location,
            'days': min(days, 10)
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao obter previsão: {e}")
            return None
    
    def get_historical_precipitation(self, start_date, end_date):
        """
        Args:
            start_date (str): Data inicial no formato 'YYYY-MM-DD'
            end_date (str): Data final no formato 'YYYY-MM-DD'
        Returns:
            list: Lista de dados históricos ou None se houver erro
        """
        historical_data = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_dt:
            url = f"{self.base_url}/history.json"
            params = {
                'key': self.api_key,
                'q': self.location,
                'dt': current_date.strftime('%Y-%m-%d')
            }
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                historical_data.append(response.json())
                print(f"Dados de precipitação obtidos para {current_date.strftime('%Y-%m-%d')}")
                time.sleep(0.1)  # Evita rate limiting
            except requests.exceptions.RequestException as e:
                print(f"Erro ao obter dados históricos para {current_date.strftime('%Y-%m-%d')}: {e}")
            
            current_date += timedelta(days=1)
        
        return historical_data if historical_data else None
    
    def get_multiple_days_hourly_precipitation(self, start_date, end_date):
        """
        Args:
            start_date (str): Data inicial no formato 'YYYY-MM-DD'  
            end_date (str): Data final no formato 'YYYY-MM-DD'   
        Returns:
            list: Lista de dados horários ou None se houver erro
        """
        hourly_data_list = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_dt:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"Obtendo dados horários para {date_str}...")
            
            url = f"{self.base_url}/history.json"
            params = {
                'key': self.api_key,
                'q': self.location,
                'dt': date_str
            }
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                day_data = response.json()
                
                # Extrai dados horários do dia
                if 'forecast' in day_data and 'forecastday' in day_data['forecast']:
                    forecast_day = day_data['forecast']['forecastday'][0]
                    location = day_data['location']
                    
                    if 'hour' in forecast_day and forecast_day['hour']:
                        for hour_data in forecast_day['hour']:
                            hourly_info = {
                                'data': date_str,
                                'hora': hour_data['time'].split(' ')[1],
                                'datetime_sort': hour_data['time'],
                                'precipitacao_mm': hour_data['precip_mm'],
                                'umidade_percent': hour_data['humidity'],
                                'nuvens_percent': hour_data['cloud'],
                                'condicao_tempo': hour_data['condition']['text'],
                                'chance_chuva': hour_data.get('chance_of_rain', 0)
                            }
                            hourly_data_list.append(hourly_info)
                
                time.sleep(0.1)  # Evita rate limiting
                
            except requests.exceptions.RequestException as e:
                print(f"Erro ao obter dados horários para {date_str}: {e}")
            
            current_date += timedelta(days=1)
        
        return hourly_data_list if hourly_data_list else None
    def get_hourly_precipitation(self, date):
        """
        Args:
            date (str): Data no formato 'YYYY-MM-DD'
        Returns:
            dict: Dados horários ou None se houver erro
        """
        url = f"{self.base_url}/history.json"
        params = {
            'key': self.api_key,
            'q': self.location,
            'dt': date
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao obter dados horários para {date}: {e}")
            return None
    
    def format_multiple_days_hourly_precipitation(self, hourly_data_list):
        """
        Args:
            hourly_data_list (list): Lista de dados horários
            
        Returns:
            pd.DataFrame: DataFrame com dados horários organizados
        """
        if not hourly_data_list:
            return pd.DataFrame()
        
        df = pd.DataFrame(hourly_data_list)
        
        # Ordena por data e hora usando o campo datetime_sort
        if not df.empty and 'datetime_sort' in df.columns:
            df = df.sort_values('datetime_sort').reset_index(drop=True)
            # Remove o campo auxiliar antes de retornar
            df = df.drop('datetime_sort', axis=1)
        
        return df
    
    def format_current_precipitation(self, data):
        """
        Args:
            data (dict): Dados brutos da API
            
        Returns:
            pd.DataFrame: DataFrame com dados de precipitação
        """
        if not data:
            return pd.DataFrame()
        
        current = data['current']
        location = data['location']
        
        precipitation_data = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data': datetime.now().strftime('%Y-%m-%d'),
            'cidade': location['name'],
            'precipitacao_mm': current['precip_mm'],
            'precipitacao_in': current['precip_in'],
            'umidade_percent': current['humidity'],
            'nuvens_percent': current['cloud'],
            'condicao_tempo': current['condition']['text'],
            'chance_chuva': 'N/A',  # Não disponível para dados atuais
            'tipo_dados': 'Atual'
        }
        
        return pd.DataFrame([precipitation_data])
    
    def format_forecast_precipitation(self, data):
        """
        Args:
            data (dict): Dados brutos da API
            
        Returns:
            pd.DataFrame: DataFrame com previsão de precipitação
        """
        if not data:
            return pd.DataFrame()
        
        precipitation_list = []
        location = data['location']
        
        for day in data['forecast']['forecastday']:
            day_data = day['day']
            
            precipitation_data = {
                'data': day['date'],
                'cidade': location['name'],
                'precipitacao_mm': day_data['totalprecip_mm'],
                'precipitacao_in': day_data['totalprecip_in'],
                'umidade_percent': day_data['avghumidity'],
                'nuvens_percent': 'N/A',  # Não disponível na previsão diária
                'condicao_tempo': day_data['condition']['text'],
                'chance_chuva': day_data.get('daily_chance_of_rain', 0),
                'tipo_dados': 'Previsão'
            }
            
            precipitation_list.append(precipitation_data)
        
        return pd.DataFrame(precipitation_list)
    
    def format_historical_precipitation(self, data_list):
        """
        Args:
            data_list (list): Lista de dados brutos da API
            
        Returns:
            pd.DataFrame: DataFrame com histórico de precipitação
        """
        if not data_list:
            return pd.DataFrame()
        
        precipitation_list = []
        
        for data in data_list:
            location = data['location']
            forecast_day = data['forecast']['forecastday'][0]
            day_data = forecast_day['day']
            
            precipitation_data = {
                'data': forecast_day['date'],
                'cidade': location['name'],
                'precipitacao_mm': day_data['totalprecip_mm'],
                'precipitacao_in': day_data['totalprecip_in'],
                #'umidade_percent': day_data['avghumidade'],
                'nuvens_percent': 'N/A',
                'condicao_tempo': day_data['condition']['text'],
                'chance_chuva': 'N/A',  # Não disponível para dados históricos
                'tipo_dados': 'Histórico'
            }
            
            precipitation_list.append(precipitation_data)
        
        return pd.DataFrame(precipitation_list)
    
    def format_hourly_precipitation(self, data, date):
        """
        Args:
            data (dict): Dados brutos da API
            date (str): Data dos dados
            
        Returns:
            pd.DataFrame: DataFrame com dados horários
        """
        if not data:
            return pd.DataFrame()
        
        precipitation_list = []
        location = data['location']
        forecast_day = data['forecast']['forecastday'][0]
        
        for hour_data in forecast_day['hour']:
            precipitation_data = {
                'data': date,
                'hora': hour_data['time'].split(' ')[1],
                'datetime_sort': hour_data['time'],  # Campo auxiliar para ordenação
                'precipitacao_mm': hour_data['precip_mm'],
                'umidade_percent': hour_data['humidity'],
                'nuvens_percent': hour_data['cloud'],
                'condicao_tempo': hour_data['condition']['text'],
                'chance_chuva': hour_data.get('chance_of_rain', 0)
            }
            
            precipitation_list.append(precipitation_data)
        
        return pd.DataFrame(precipitation_list)
    
    def create_precipitation_summary(self, df):
        """
        Args:
            df (pd.DataFrame): DataFrame com dados de precipitação
            
        Returns:
            pd.DataFrame: DataFrame com resumo estatístico
        """
        if df.empty:
            return pd.DataFrame()
        
        # Filtra apenas dados numéricos de precipitação
        precip_data = df[df['precipitacao_mm'].notna() & (df['precipitacao_mm'] > 0)]
        
        if precip_data.empty:
            summary_data = {
                'metrica': ['Dias com dados', 'Dias com chuva', 'Precipitação total (mm)', 
                           'Precipitação média (mm)', 'Precipitação máxima (mm)', 'Precipitação mínima (mm)'],
                'valor': [len(df), 0, 0, 0, 0, 0]
            }
        else:
            summary_data = {
                'metrica': ['Dias com dados', 'Dias com chuva', 'Precipitação total (mm)', 
                           'Precipitação média (mm)', 'Precipitação máxima (mm)', 'Precipitação mínima (mm)'],
                'valor': [
                    len(df),
                    len(precip_data),
                    round(precip_data['precipitacao_mm'].sum(), 2),
                    round(precip_data['precipitacao_mm'].mean(), 2),
                    round(precip_data['precipitacao_mm'].max(), 2),
                    round(precip_data['precipitacao_mm'].min(), 2)
                ]
            }
        
        return pd.DataFrame(summary_data)
    
    def save_precipitation_data(self, dataframes_dict, filename):
        """
        Args:
            dataframes_dict (dict): Dicionário com DataFrames
            filename (str): Nome do arquivo (usa padrão se None)
        """
        if filename is None:
            filename = "precipitacao_sao_paulo.xlsx"
        
        try:
            import os
            from openpyxl import load_workbook
            
            # Cria o diretório se não existir
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            # Verifica se o arquivo já existe
            if os.path.exists(filename):
                print(f"Arquivo existente encontrado: {filename}")
                print("Anexando novos dados aos existentes...")
                
                # Lê dados existentes
                existing_data = {}
                try:
                    with pd.ExcelFile(filename) as xls:
                        for sheet_name in xls.sheet_names:
                            existing_data[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
                            print(f"Dados existentes na aba '{sheet_name}': {len(existing_data[sheet_name])} registros")
                except Exception as e:
                    print(f"Erro ao ler arquivo existente: {e}")
                    existing_data = {}
                
                # Combina dados existentes com novos
                combined_data = {}
                for sheet_name, new_df in dataframes_dict.items():
                    if sheet_name in existing_data and not existing_data[sheet_name].empty:
                        if sheet_name == 'Resumo_Precipitacao':
                            # Para resumo, substitui os dados (não anexa)
                            combined_data[sheet_name] = new_df
                        else:
                            # Para outros dados, remove duplicatas e anexa
                            existing_df = existing_data[sheet_name]
                            
                            # Remove duplicatas baseado na data (e hora se existir)
                            if 'data' in new_df.columns and 'hora' in new_df.columns and 'data' in existing_df.columns and 'hora' in existing_df.columns:
                                # Para dados horários - usa combinação de data + hora para identificar duplicatas
                                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['data', 'hora'], keep='last')
                                
                                # Ordena cronologicamente por data e hora
                                combined_df['datetime_temp'] = pd.to_datetime(combined_df['data'] + ' ' + combined_df['hora'])
                                combined_df = combined_df.sort_values('datetime_temp').reset_index(drop=True)
                                combined_df = combined_df.drop('datetime_temp', axis=1)
                                
                            elif 'data' in new_df.columns and 'data' in existing_df.columns:
                                # Para dados diários
                                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                                combined_df = combined_df.drop_duplicates(subset=['data'], keep='last')
                                combined_df = combined_df.sort_values('data').reset_index(drop=True)
                            else:
                                # Anexa sem verificar duplicatas mas ordena se possível
                                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                                if 'data' in combined_df.columns and 'hora' in combined_df.columns:
                                    combined_df['datetime_temp'] = pd.to_datetime(combined_df['data'] + ' ' + combined_df['hora'])
                                    combined_df = combined_df.sort_values('datetime_temp').reset_index(drop=True)
                                    combined_df = combined_df.drop('datetime_temp', axis=1)
                                elif 'data' in combined_df.columns:
                                    combined_df = combined_df.sort_values('data').reset_index(drop=True)
                            
                            combined_data[sheet_name] = combined_df
                    else:
                        combined_data[sheet_name] = new_df
                
                dataframes_dict = combined_data
            else:
                print(f"Criando novo arquivo: {filename}")
            
            # Salva todos os dados
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                for sheet_name, df in dataframes_dict.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"Aba '{sheet_name}' salva com {len(df)} registros")
                    else:
                        print(f"Aba '{sheet_name}' está vazia")
            
            print(f"\nDados de precipitação salvos em: {filename}")
            
        except Exception as e:
            print(f"Erro ao salvar arquivo: {e}")
            print("Tentando salvar com timestamp como backup...")
            backup_filename = f"precipitacao_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            try:
                with pd.ExcelWriter(backup_filename, engine='openpyxl') as writer:
                    for sheet_name, df in dataframes_dict.items():
                        if not df.empty:
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Backup salvo em: {backup_filename}")
            except Exception as backup_error:
                print(f"Erro ao salvar backup: {backup_error}")
    
    def extract_precipitation_data(self, historical_days=7, forecast_days=7, include_hourly=False):
        """
        Args:
            historical_days (int): Número de dias históricos
            forecast_days (int): Número de dias de previsão
            include_hourly (bool): Se deve incluir dados horários de hoje
            
        Returns:
            dict: Dicionário com DataFrames de precipitação
        """
        print("Iniciando extração de dados de precipitação...")
        
        # Dados atuais
        print("Obtendo precipitação atual...")
        current_data = self.get_current_precipitation()
        current_df = self.format_current_precipitation(current_data)
        
        # Previsão
        print(f"Obtendo previsão de precipitação para {forecast_days} dias...")
        forecast_data = self.get_forecast_precipitation(forecast_days)
        forecast_df = self.format_forecast_precipitation(forecast_data)
        
        # Dados históricos
        print(f"Obtendo histórico de precipitação dos últimos {historical_days} dias...")
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=historical_days-1)
        
        historical_data = self.get_historical_precipitation(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        historical_df = self.format_historical_precipitation(historical_data)
        
        # Combina todos os dados diários e ordena cronologicamente
        all_daily_data = pd.concat([historical_df, current_df, forecast_df], ignore_index=True)
        if not all_daily_data.empty and 'data' in all_daily_data.columns:
            all_daily_data = all_daily_data.sort_values('data').reset_index(drop=True)
        
        # Dados horários (opcional) - ordenados cronologicamente
        hourly_df = pd.DataFrame()
        if include_hourly:
            print("Obtendo dados horários de precipitação de hoje...")
            today = datetime.now().strftime('%Y-%m-%d')
            # Obtém dados horários dos últimos dias (incluindo hoje)
            end_date_hourly = datetime.now()
            start_date_hourly = end_date_hourly - timedelta(days=7)  # Últimos 7 dias
            
            hourly_data_list = self.get_multiple_days_hourly_precipitation(
                start_date_hourly.strftime('%Y-%m-%d'),
                end_date_hourly.strftime('%Y-%m-%d')
            )
            hourly_df = self.format_multiple_days_hourly_precipitation(hourly_data_list)
        
        # Cria resumo estatístico
        summary_df = self.create_precipitation_summary(all_daily_data)
        
        # Organiza DataFrames (todos ordenados cronologicamente)
        dataframes = {
            'Resumo_Precipitacao': summary_df,
            'Dados_Diarios': all_daily_data,
            'Dados_Historicos': historical_df.sort_values('data').reset_index(drop=True) if not historical_df.empty and 'data' in historical_df.columns else historical_df,
            'Dados_Atuais': current_df,
            'Previsao_Precipitacao': forecast_df.sort_values('data').reset_index(drop=True) if not forecast_df.empty and 'data' in forecast_df.columns else forecast_df
        }
        
        if not hourly_df.empty:
            dataframes['Dados_Horarios'] = hourly_df
        
        # Salva automaticamente no arquivo fixo
        filename = "precipitacao_sao_paulo.xlsx"
        self.save_precipitation_data(dataframes, filename)
        
        print("Extração de dados de precipitação concluída!")
        return dataframes
    
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

def AccuWeather():
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


# Exemplo de uso
if __name__ == "__main__":
    # IMPORTANTE: Substitua pela sua chave da API
    API_KEY = "1c46ec42df784b8190e190658252508"
    
    if API_KEY == "SUA_CHAVE_API_AQUI":
        print("ERRO: Você precisa definir sua chave da API do WeatherAPI.com")
        print("1. Acesse: https://www.weatherapi.com/")
        print("2. Crie uma conta gratuita")
        print("3. Obtenha sua chave da API")
        print("4. Substitua 'SUA_CHAVE_API_AQUI' pela sua chave")
    else:
        # Cria o extrator de precipitação
        extractor = PrecipitationDataExtractor(API_KEY)
        
        # Extrai dados de precipitação
        precipitation_data = extractor.extract_precipitation_data(
            historical_days=30,     # Últimos 30 dias
            forecast_days=7,        # Próximos 7 dias
            include_hourly=True     # Inclui dados horários de hoje
        )
        
        # Exemplo de uso individual:
        
        # Apenas dados atuais de precipitação
        # current_data = extractor.get_current_precipitation()
        # current_df = extractor.format_current_precipitation(current_data)
        
        # Apenas previsão de precipitação
        # forecast_data = extractor.get_forecast_precipitation(5)
        # forecast_df = extractor.format_forecast_precipitation(forecast_data)
        
        # Dados horários de uma data específica
        # hourly_data = extractor.get_hourly_precipitation('2024-08-25')
        # hourly_df = extractor.format_hourly_precipitation(hourly_data, '2024-08-25')
        
        # Salvar dados em arquivo específico
        filename = "precipitacao_sao_paulo.xlsx"
        extractor.save_precipitation_data(precipitation_data, filename)

        # Para executar com menu interativo:
    AccuWeather()
    
    # Para executar diretamente em background por 24 horas, descomente:
    # run_background(24)