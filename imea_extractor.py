#!/usr/bin/env python3
"""
IMEA Direct Data Extractor - Simplified Version
Extracts agricultural data from IMEA API and saves to CSV files

Usage: python imea_extractor.py
Output: CSV files in ./datasets/ directory
"""

import os
import sys
import pandas as pd
import requests
import ssl
import urllib3
import logging
from datetime import datetime, timedelta
import concurrent.futures
from typing import Dict, List, Optional, Any

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️ python-dotenv not installed. Install with: pip install python-dotenv")

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TLSAdapter(requests.adapters.HTTPAdapter):
    """Custom TLS adapter for IMEA's SSL configuration"""
    
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= 0x4
        kwargs['ssl_context'] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

class IMEAExtractor:
    """Simplified IMEA Data Extractor"""
    
    def __init__(self):
        self.base_url = 'https://api1.imea.com.br'
        self.access_token = None
        
        # Setup session with custom TLS adapter
        self.session = requests.Session()
        self.session.mount('https://', TLSAdapter())
        
        # IMEA data mapping
        self.chains = {
            '1': {'name': 'Cotton', 'code': 'CTN'},
            '3': {'name': 'Corn', 'code': 'CRN'}, 
            '4': {'name': 'Soy', 'code': 'SOY'}
        }
        
        self.groups = {
            '697311317415952386': {'name': 'Planting', 'code': 'PLT'},
            '697311317415952384': {'name': 'Harvest', 'code': 'HRV'},
            '697311317415952385': {'name': 'Commercialization', 'code': 'COM'}
        }
        
        self.states = {
            '51': {'name': 'Mato Grosso', 'code': 'MT'}
        }
        
        # Ensure datasets directory exists
        os.makedirs('datasets', exist_ok=True)
        
    def authenticate(self) -> bool:
        """Authenticate with IMEA API"""
        username = os.getenv('IMEA_USERNAME')
        password = os.getenv('IMEA_PASSWORD')
        
        if not username or not password:
            logger.error("❌ IMEA credentials not found in environment variables")
            logger.error("Please set IMEA_USERNAME and IMEA_PASSWORD in .env file")
            return False
            
        try:
            logger.info("🔐 Authenticating with IMEA API...")
            
            headers = {
                'content-type': 'application/x-www-form-urlencoded',
                'authorization': 'bearer undefined'
            }
            
            auth_data = {
                'username': username,
                'password': password,
                'grant_type': 'password',
                'client_id': '2',
            }
            
            response = self.session.post(
                f'{self.base_url}/token',
                headers=headers,
                data=auth_data,
                verify=False
            )
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data.get('access_token')
                
                if self.access_token:
                    logger.info("✅ Authentication successful!")
                    return True
                else:
                    logger.error("❌ No access token in response")
                    
            else:
                logger.error(f"❌ Authentication failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ Authentication error: {e}")
            
        return False
    
    def extract_historical_series(self) -> Optional[pd.DataFrame]:
        """Extract historical series data"""
        if not self.access_token:
            logger.error("❌ No access token available")
            return None
            
        logger.info("📈 Extracting historical agricultural data...")
        
        # Define date range (from 2021 to present + 3 months)
        start_dt = datetime(2021, 10, 1)
        end_dt = datetime.now() + timedelta(days=90)
        
        # Generate all requests
        all_requests = []
        
        for chain_id in ['4', '3', '1']:  # Soy, Corn, Cotton
            for group_id in ['697311317415952386', '697311317415952384', '697311317415952385']:
                crop_name = self.chains.get(chain_id, {}).get('name', f'Chain {chain_id}')
                activity_name = self.groups.get(group_id, {}).get('name', f'Group {group_id}')
                
                # Generate yearly requests
                for year in range(start_dt.year, end_dt.year + 1):
                    year_start = datetime(year, 1, 1)
                    year_end = datetime(year, 12, 31)
                    
                    if year_start < start_dt:
                        year_start = start_dt
                    if year_end > end_dt:
                        year_end = end_dt
                    
                    request_info = {
                        'chain_id': chain_id,
                        'group_id': group_id,
                        'crop_name': crop_name,
                        'activity_name': activity_name,
                        'start_date': year_start.strftime('%Y-%m-%d'),
                        'end_date': year_end.strftime('%Y-%m-%d'),
                        'year': year
                    }
                    all_requests.append(request_info)
        
        logger.info(f"🚀 Making {len(all_requests)} parallel requests...")
        
        # Execute requests in parallel
        all_data = []
        
        def make_request(request_info):
            payload = {
                'inicio': request_info['start_date'],
                'fim': request_info['end_date'],
                'cadeia': [request_info['chain_id']],
                'grupo': [request_info['group_id']],
                'indicador': [],
                'tipolocalidade': ['1'],
                'estado': ['51'],
                'regiao': [],
                'cidade': [],
                'tipoDestino': [],
                'estadoDestino': [],
                'regiaoDestino': [],
                'cidadeDestino': [],
                'safra': [],
            }
    
            headers = {
                'authorization': f'bearer {self.access_token}',
                'content-type': 'application/json;charset=UTF-8',
                'accept': 'application/json, text/plain, */*',
            }
            
            try:
                response = self.session.post(
                    f'{self.base_url}/api/seriehistorica/export',
                    headers=headers,
                    json=payload,
                    verify=False,
                    timeout=30
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and data:
                        return {'success': True, 'data': data}
                    else:
                        return {'success': True, 'data': []}
                
                return {'success': False, 'error': f"HTTP {response.status_code}"}
                
            except Exception as e:
                return {'success': False, 'error': str(e)}
        
        # Execute with ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(make_request, req) for req in all_requests]
            
            completed = 0
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                completed += 1
                
                if result['success']:
                    all_data.extend(result['data'])
                    if completed % 10 == 0:
                        logger.info(f"✅ Completed {completed}/{len(all_requests)} requests...")
        
        logger.info(f"🎯 Requests completed: {completed}/{len(all_requests)}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"✅ Retrieved: {len(df)} historical records")
            
            # Transform data
            return self._transform_data(df)
        else:
            logger.warning("⚠️ No historical data retrieved")
            return None
    
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform raw data into clean format"""
        try:
            # Convert date column
            df['Data'] = pd.to_datetime(df['Data'])
            df['date'] = df['Data'].dt.date
            df['year'] = df['Data'].dt.year
            df['month'] = df['Data'].dt.month
            
            # Convert numeric values (percentages)
            df['percentage'] = pd.to_numeric(df['Valor'], errors='coerce')
            
            # Add readable names
            df['crop'] = df['CadeiaId'].map(lambda x: self.chains.get(x, {}).get('name', f'Chain {x}'))
            df['activity'] = df['IndicadorGrupoId'].map(lambda x: self.groups.get(x, {}).get('name', f'Group {x}'))
            df['state'] = df['EstadoId'].map(lambda x: self.states.get(x, {}).get('name', f'State {x}'))
            df['harvest_season'] = df['SafraDescricao']
            
            # Filter only percentage data
            if 'UnidadeDescricao' in df.columns:
                df = df[df['UnidadeDescricao'] == 'Percentual'].copy()
            
            # Create final structure
            final_df = df[[
                'date', 'year', 'month', 'crop', 'activity', 'percentage', 
                'harvest_season', 'state'
            ]].copy()
            
            # Add metadata
            final_df['extraction_date'] = datetime.now()
            final_df['data_source'] = 'IMEA_API'
            
            logger.info(f"✅ Data transformed: {len(final_df)} percentage records")
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Error transforming data: {e}")
            return df
    
    def create_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create summary with all activities in one row"""
        try:
            # Pivot data
            summary = df.pivot_table(
                index=['date', 'year', 'month', 'crop', 'state', 'harvest_season'],
                columns='activity',
                values='percentage',
                aggfunc='mean'
            ).reset_index()
            
            # Flatten column names
            summary.columns.name = None
            
            # Rename columns
            column_rename = {
                'Planting': 'planted_percentage',
                'Harvest': 'harvested_percentage',
                'Commercialization': 'commercialized_percentage'
            }
            summary = summary.rename(columns=column_rename)
            
            # Fill missing values with 0
            for col in ['planted_percentage', 'harvested_percentage', 'commercialized_percentage']:
                if col not in summary.columns:
                    summary[col] = 0.0
                else:
                    summary[col] = summary[col].fillna(0.0)
            
            # Reorder columns
            final_columns = [
                'date', 'year', 'month', 'crop', 'state', 'harvest_season',
                'planted_percentage', 'harvested_percentage', 'commercialized_percentage'
            ]
            
            summary = summary[final_columns]
            summary = summary.sort_values(['date', 'crop']).reset_index(drop=True)
            
            logger.info(f"✅ Created summary with {len(summary)} records")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Error creating summary: {e}")
            return df
    
    def save_datasets(self, df: pd.DataFrame) -> None:
        """Save data as CSV files"""
        try:
            # Save main summary file
            summary_df = self.create_summary(df)
            summary_path = 'datasets/BR_IMEA_CROP_PERCENTAGE_PROGRESS.csv'
            summary_df.to_csv(summary_path, index=False)
            logger.info(f"📊 Saved main summary: {len(summary_df)} records → {summary_path}")
            
            # Save individual crop/activity files
            crops = ['Soy', 'Corn', 'Cotton']
            activities = [
                ('Planting', 'planted_percentage', 'PLANTING'),
                ('Harvest', 'harvested_percentage', 'HARVEST'), 
                ('Commercialization', 'commercialized_percentage', 'COMMERCIALIZATION')
            ]
            
            saved_files = []
            
            for crop in crops:
                crop_data = summary_df[summary_df['crop'] == crop].copy()
                
                if crop_data.empty:
                    continue
                
                for activity_name, percentage_col, file_suffix in activities:
                    activity_data = crop_data[crop_data[percentage_col] > 0].copy()
                    
                    if activity_data.empty:
                        continue
                    
                    # Create clean dataset
                    clean_data = activity_data[[
                        'date', 'year', 'month', 'crop', 'state', 'harvest_season', percentage_col
                    ]].copy()
                    
                    clean_data = clean_data.rename(columns={percentage_col: 'percentage'})
                    clean_data = clean_data.sort_values('date').reset_index(drop=True)
                    
                    # Save file
                    filename = f'BR_IMEA_{crop.upper()}_{file_suffix}_PERCENTAGE.csv'
                    file_path = f'datasets/{filename}'
                    clean_data.to_csv(file_path, index=False)
                    saved_files.append((filename, len(clean_data)))
                    
                    logger.info(f"📄 Saved {crop} {activity_name}: {len(clean_data)} records → {filename}")
            
            # Print summary
            total_files = len(saved_files) + 1  # +1 for main summary
            total_records = sum(count for _, count in saved_files) + len(summary_df)
            
            logger.info(f"\n📋 EXTRACTION SUMMARY:")
            logger.info(f"✅ Total files created: {total_files}")
            logger.info(f"✅ Total records: {total_records:,}")
            logger.info(f"✅ Date range: {summary_df['date'].min()} to {summary_df['date'].max()}")
            logger.info(f"✅ All files saved to: ./datasets/")
            
        except Exception as e:
            logger.error(f"❌ Error saving datasets: {e}")
    
    def run(self) -> bool:
        """Main extraction process"""
        logger.info("🌾 Starting IMEA Data Extraction...")
        
        # Authenticate
        if not self.authenticate():
            return False
        
        # Extract data
        df = self.extract_historical_series()
        if df is None or df.empty:
            logger.error("❌ No data extracted")
            return False
        
        # Save datasets
        self.save_datasets(df)
        
        logger.info("🎉 Data extraction completed successfully!")
        return True

def main():
    """Main function"""
    print("🌾 IMEA Direct Data Extractor")
    print("=" * 40)
    
    extractor = IMEAExtractor()
    success = extractor.run()
    
    if success:
        print("\n✅ Success! Check the ./datasets/ folder for your CSV files")
        print("📊 Files ready for analysis in Excel, Python, R, or any data tool")
    else:
        print("\n❌ Extraction failed. Check the logs above for details.")
        sys.exit(1)

if __name__ == "__main__":
    main() 