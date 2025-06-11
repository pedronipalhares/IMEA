#!/usr/bin/env python3
"""
IMEA Direct Data Extractor
Extracts agricultural data from IMEA API using direct endpoints (no email workflow)

This extractor uses the discovered working endpoints:
1. /api/safra/seriehistoricageral - for harvest/season IDs
2. /api/seriehistorica - for historical series data with specific indicators
"""

from typing import Dict, List, Optional, Any
import pandas as pd
import requests
import ssl
import urllib3
import logging
from datetime import datetime, timedelta
import json
import asyncio
import aiohttp
import concurrent.futures
import os
import sys

# Try to import dateutil, fall back to manual month calculation if not available
try:
    from dateutil.relativedelta import relativedelta
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False

# Standalone base class for testing
class BaseExtractor:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Dataset management functions
def ensure_datasets_dir() -> str:
    """
    Ensure the datasets directory exists and return its path
    
    Returns:
        str: Path to the datasets directory
    """
    datasets_dir = os.path.join(os.getcwd(), 'datasets')
    os.makedirs(datasets_dir, exist_ok=True)
    return datasets_dir

def save_dataset(df: pd.DataFrame, filename: str, index: bool = False) -> str:
    """
    Save a DataFrame to a CSV file in the datasets directory
    
    Args:
        df: DataFrame to save
        filename: Name of the CSV file
        index: Whether to include index in the CSV
        
    Returns:
        str: Full path to the saved file
    """
    datasets_dir = ensure_datasets_dir()
    file_path = os.path.join(datasets_dir, filename)
    
    try:
        df.to_csv(file_path, index=index)
        logging.info(f"✅ Dataset saved: {len(df)} records → {filename}")
        return file_path
    except Exception as e:
        logging.error(f"❌ Failed to save dataset {filename}: {e}")
        raise

class TLSAdapter(requests.adapters.HTTPAdapter):
    """Custom TLS adapter for IMEA's SSL configuration"""
    
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= 0x4
        kwargs['ssl_context'] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

class IMEADirectExtractor(BaseExtractor):
    """
    IMEA Direct Data Extractor
    
    Extracts agricultural data from IMEA using direct API endpoints:
    - Historical series data (planting, harvest, commercialization) using specific indicators
    - Current price data for different crops
    """
    
    def __init__(self, credentials: Dict[str, str]):
        super().__init__()
        self.name = "IMEA Direct Extractor"
        self.base_url = 'https://api1.imea.com.br'
        self.credentials = credentials
        self.filename = 'BR_IMEA_CROP_PERCENTAGE_PROGRESS.csv'
        
        # Setup session with custom TLS adapter
        self.session = requests.Session()
        self.session.mount('https://', TLSAdapter())
        
        # IMEA data mapping with specific indicator IDs for each commodity-activity combination
        self.indicators = {
            # Cotton
            'cotton_planting': {'id': '705576963633053696', 'name': 'Cotton Planting', 'crop': 'Cotton', 'activity': 'Planting'},
            'cotton_harvest': {'id': '703492383711166464', 'name': 'Cotton Harvesting', 'crop': 'Cotton', 'activity': 'Harvest'},
            'cotton_commercialization': {'id': '703126874901708800', 'name': 'Cotton Commercialization', 'crop': 'Cotton', 'activity': 'Commercialization'},
            
            # Corn
            'corn_planting': {'id': '701211800784076800', 'name': 'Corn Planting', 'crop': 'Corn', 'activity': 'Planting'},
            'corn_harvest': {'id': '708192508847325187', 'name': 'Corn Harvesting', 'crop': 'Corn', 'activity': 'Harvest'},
            'corn_commercialization': {'id': '698758563422273536', 'name': 'Corn Commercialization', 'crop': 'Corn', 'activity': 'Commercialization'},
            
            # Soy
            'soy_planting': {'id': '708192508889268224', 'name': 'Soy Planting', 'crop': 'Soy', 'activity': 'Planting'},
            'soy_harvest': {'id': '708192508847325188', 'name': 'Soy Harvest', 'crop': 'Soy', 'activity': 'Harvest'},
            'soy_commercialization': {'id': '702389895595556864', 'name': 'Soy Commercialization', 'crop': 'Soy', 'activity': 'Commercialization'},
        }
        
        self.states = {
            '51': {'name': 'Mato Grosso', 'code': 'MT'}
        }
        
        self.access_token = None
        self.harvest_seasons = []
        
    def authenticate(self) -> bool:
        """Authenticate with IMEA API"""
        try:
            self.logger.info("🔐 Authenticating with IMEA API...")
            
            headers = {
                'content-type': 'application/x-www-form-urlencoded',
                'authorization': 'bearer undefined'
            }
            
            auth_data = {
                'username': self.credentials['username'],
                'password': self.credentials['password'],
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
                    self.logger.info("✅ Authentication successful!")
                    return True
                else:
                    self.logger.error("❌ No access token in response")
                    
            else:
                self.logger.error(f"❌ Authentication failed: {response.status_code}")
                self.logger.error(f"Response: {response.text}")
                
        except Exception as e:
            self.logger.error(f"❌ Authentication error: {e}")
            
        return False

    def fetch_harvest_seasons(self) -> List[Dict]:
        """
        Fetch all available harvest seasons using the seriehistoricageral endpoint
        
        Returns:
            List of harvest season data
        """
        if not self.access_token:
            self.logger.error("❌ No access token available")
            return []
            
        self.logger.info("📅 Fetching harvest seasons...")
        
        headers = {
            'accept': 'application/json, text/plain, */*',
            'authorization': f'bearer {self.access_token}',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://portal.imea.com.br',
            'referer': 'https://portal.imea.com.br/',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'
        }
        
        payload = {
            "nome": "",
            "pageSize": 100,  # Get more records
            "page": 1,
            "cadeia": [],
            "grupo": [],
            "indicador": [],
            "estado": [],
            "regiao": [],
            "cidade": []
        }
        
        try:
            response = self.session.post(
                f'{self.base_url}/api/safra/seriehistoricageral',
                headers=headers,
                json=payload,
                verify=False,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Handle both direct list and paginated response
                if isinstance(data, list):
                    seasons = data
                elif isinstance(data, dict) and 'data' in data:
                    seasons = data['data']
                else:
                    seasons = []
                
                self.harvest_seasons = seasons
                self.logger.info(f"✅ Retrieved {len(seasons)} harvest seasons")
                
                # Log season details
                for season in seasons[:10]:  # Show first 10
                    season_id = season.get('Id')
                    season_name = season.get('Nome', 'Unknown')
                    self.logger.info(f"   Season: {season_name} (ID: {season_id})")
                
                return seasons
                
            else:
                self.logger.error(f"❌ Failed to fetch harvest seasons: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"❌ Error fetching harvest seasons: {e}")
            return []

    def extract_historical_series(self, 
                                 start_date: Optional[str] = None,
                                 end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Extract historical series data using specific indicator IDs with monthly granular requests
        
        Makes individual monthly requests per commodity per indicator to ensure complete data coverage
        while using tipolocalidade=1 for proper data filtering.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with historical series data
        """
        if not self.access_token:
            self.logger.error("❌ No access token available")
            return None
            
        self.logger.info("🚀 Extracting historical series data with MONTHLY granular requests and tipolocalidade=1...")
        
        # Fetch harvest seasons first
        harvest_seasons = self.fetch_harvest_seasons()
        if not harvest_seasons:
            self.logger.warning("⚠️ No harvest seasons found, proceeding without season filtering")
        
        # Extract season IDs
        season_ids = [season.get('Id') for season in harvest_seasons if season.get('Id')]
        self.logger.info(f"📅 Using {len(season_ids)} harvest seasons")
        
        # Define overall date range - start from 2021 for comprehensive data
        from datetime import datetime, timedelta
        
        if start_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        else:
            start_dt = datetime(2021, 1, 1)  # Start from 2021 for comprehensive coverage
            
        if end_date:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_dt = datetime.now() + timedelta(days=90)  # Include current + next 3 months
        
        self.logger.info(f"📅 Overall date range: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
        
        # Generate monthly date ranges for granular requests
        def generate_monthly_ranges(start_dt, end_dt):
            """Generate monthly date ranges for individual API requests"""
            ranges = []
            current_month = start_dt.replace(day=1)  # Start at beginning of month
            
            while current_month <= end_dt:
                # Calculate end of current month
                if HAS_DATEUTIL:
                    next_month = current_month + relativedelta(months=1)
                else:
                    # Manual month calculation
                    if current_month.month == 12:
                        next_month = current_month.replace(year=current_month.year + 1, month=1)
                    else:
                        next_month = current_month.replace(month=current_month.month + 1)
                
                month_end = next_month - timedelta(days=1)
                
                # Don't exceed overall end date
                actual_end = min(month_end, end_dt)
                
                ranges.append((current_month, actual_end))
                current_month = next_month
            
            return ranges
        
        # Generate monthly date ranges for optimal granularity
        monthly_ranges = generate_monthly_ranges(start_dt, end_dt)
        self.logger.info(f"📅 Generated {len(monthly_ranges)} monthly ranges for comprehensive extraction")
        
        # Create requests for each indicator for each month
        all_requests = []
        
        for indicator_key, indicator_info in self.indicators.items():
            for range_start, range_end in monthly_ranges:
                request_info = {
                    'indicator_key': indicator_key,
                    'indicator_id': indicator_info['id'],
                    'crop': indicator_info['crop'],
                    'activity': indicator_info['activity'],
                    'start_date': range_start.strftime('%Y-%m-%d'),
                    'end_date': range_end.strftime('%Y-%m-%d'),
                    'month_label': range_start.strftime('%Y-%m'),
                    'range_label': f"{range_start.strftime('%Y-%m-%d')} to {range_end.strftime('%Y-%m-%d')}"
                }
                all_requests.append(request_info)
        
        total_requests = len(all_requests)
        months_count = len(monthly_ranges)
        self.logger.info(f"🚀 Making {total_requests} monthly requests ({len(self.indicators)} indicators × {months_count} months)")
        self.logger.info(f"⚡ Using tipolocalidade=1 for proper data filtering!")
        
        # Function to make individual requests - optimized for speed
        def make_request(request_info):
            """Make a single API request for a specific indicator and month"""
            
            payload = {
                "pageSize": 1000,  # Large page size to get all data within the month
                "inicio": request_info['start_date'],
                "fim": request_info['end_date'],
                "cadeia": [],
                "grupo": [],
                "indicador": [request_info['indicator_id']],  # Use specific indicator ID
                "tipolocalidade": ["1"],  # Always include tipolocalidade = 1
                "estado": [],  # Empty to get all states
                "regiao": [],
                "cidade": [],
                "tipoDestino": [],
                "estadoDestino": [],
                "regiaoDestino": [],
                "cidadeDestino": [],
                "safra": season_ids  # Use all available seasons
            }
            
            headers = {
                'accept': 'application/json, text/plain, */*',
                'authorization': f'bearer {self.access_token}',
                'content-type': 'application/json;charset=UTF-8',
                'origin': 'https://portal.imea.com.br',
                'referer': 'https://portal.imea.com.br/',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36'
            }
            
            try:
                response = self.session.post(
                    f'{self.base_url}/api/seriehistorica',
                    headers=headers,
                    json=payload,
                    verify=False,
                    timeout=20  # Reasonable timeout for monthly requests
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Handle both direct list and paginated response
                    if isinstance(data, list):
                        records = data
                    elif isinstance(data, dict) and 'data' in data:
                        records = data['data']
                    else:
                        records = []
                    
                    # Add metadata to each record
                    for record in records:
                        record['_crop'] = request_info['crop']
                        record['_activity'] = request_info['activity']
                        record['_indicator_key'] = request_info['indicator_key']
                        record['_month'] = request_info['month_label']
                    
                    # Log warning if we hit close to the limit
                    if len(records) >= 10:
                        self.logger.warning(f"⚠️ Hit 10-record limit: {request_info['crop']} {request_info['activity']} {request_info['month_label']}: {len(records)} records")
                    
                    return {
                        'success': True,
                        'data': records,
                        'crop': request_info['crop'],
                        'activity': request_info['activity'],
                        'month': request_info['month_label'],
                        'record_count': len(records)
                    }
                
                return {
                    'success': False, 
                    'error': f"HTTP {response.status_code}", 
                    'request_info': request_info
                }
                
            except Exception as e:
                return {
                    'success': False, 
                    'error': str(e), 
                    'request_info': request_info
                }
        
        # Execute requests with high parallelization
        all_data = []
        max_workers = 15  # High concurrency for speed
        
        import concurrent.futures
        
        # Process requests in batches for progress tracking
        batch_size = 50  # Good balance for monthly requests
        
        self.logger.info(f"⚡ Starting high-speed parallel extraction with {max_workers} workers...")
        
        for batch_start in range(0, total_requests, batch_size):
            batch_end = min(batch_start + batch_size, total_requests)
            batch_requests = all_requests[batch_start:batch_end]
            
            batch_num = batch_start // batch_size + 1
            total_batches = (total_requests - 1) // batch_size + 1
            
            self.logger.info(f"🔄 Processing batch {batch_num}/{total_batches} ({len(batch_requests)} monthly requests)")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_request = {executor.submit(make_request, req): req for req in batch_requests}
                
                batch_results = {'success': 0, 'failed': 0, 'total_records': 0, 'limit_hits': 0}
                
                for future in concurrent.futures.as_completed(future_to_request):
                    result = future.result()
                    
                    if result['success']:
                        data_count = result['record_count']
                        all_data.extend(result['data'])
                        batch_results['success'] += 1
                        batch_results['total_records'] += data_count
                        
                        # Track 10-record limit hits
                        if data_count >= 10:
                            batch_results['limit_hits'] += 1
                        
                        # Log significant results
                        if data_count > 0:
                            self.logger.info(f"✅ {result['crop']} {result['activity']} {result['month']}: {data_count} records")
                    else:
                        batch_results['failed'] += 1
                        error_info = result.get('request_info', {})
                        if batch_results['failed'] <= 5:  # Log first few failures
                            self.logger.warning(f"⚠️ Failed: {error_info.get('crop', 'Unknown')} {error_info.get('activity', 'Unknown')} {error_info.get('month_label', 'Unknown')}")
                
                self.logger.info(f"✅ Batch {batch_num} completed: {batch_results['success']} success, {batch_results['failed']} failed, {batch_results['total_records']} records")
                if batch_results['limit_hits'] > 0:
                    self.logger.warning(f"⚠️ {batch_results['limit_hits']} requests hit the 10-record limit (may be missing data)")
        
        self.logger.info(f"🎯 Completed all {total_requests} monthly requests!")
        
        # Process collected data and remove duplicates
        if all_data:
            df = pd.DataFrame(all_data)
            initial_count = len(df)
            self.logger.info(f"✅ Total retrieved: {initial_count} historical records")
            
            # Remove duplicates based on date, indicator, and value - keep the most recent
            if 'Data' in df.columns:
                df['Data'] = pd.to_datetime(df['Data'])
                # Sort before deduplication to ensure we keep the most recent data
                df = df.sort_values(['Data', '_indicator_key', '_month']).drop_duplicates(
                    subset=['Data', '_indicator_key', 'Valor'], 
                    keep='last'  # Keep the most recent duplicate
                )
                final_count = len(df)
                self.logger.info(f"🧹 After deduplication: {initial_count} → {final_count} unique records")
            
            # Transform and clean data
            df = self._transform_historical_data(df)
            return df
        else:
            self.logger.warning("⚠️ No historical data retrieved from any requests")
            return None

    def create_percentage_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a summary CSV with planted, harvested, and commercialized percentages"""
        try:
            if df.empty:
                return df
            
            # Pivot the data to have one row per date/crop/state combination
            summary = df.pivot_table(
                index=['date', 'year', 'month', 'crop', 'state', 'harvest_season'],
                columns='activity',
                values='percentage',
                aggfunc='mean'  # In case of duplicates
            ).reset_index()
            
            # Flatten column names
            summary.columns.name = None
            
            # Rename columns for clarity
            column_rename = {
                'Planting': 'planted_percentage',
                'Harvest': 'harvested_percentage',
                'Commercialization': 'commercialized_percentage'
            }
            summary = summary.rename(columns=column_rename)
            
            # Fill missing values with 0 (if an activity is not reported)
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
            
            # Sort by date and crop
            summary = summary.sort_values(['date', 'crop']).reset_index(drop=True)
            
            self.logger.info(f"✅ Created percentage summary with {len(summary)} records")
            return summary
            
        except Exception as e:
            self.logger.error(f"❌ Error creating percentage summary: {e}")
            return df

    def extract_current_prices(self, chains: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """
        Extract current price data using mobile endpoints
        
        Args:
            chains: List of chain IDs to extract ('1'=Cotton, '3'=Corn, '4'=Soy)
            
        Returns:
            DataFrame with current price data
        """
        self.logger.info("💰 Extracting current price data...")
        
        # Legacy chain mapping for price endpoints
        chain_mapping = {
            '1': {'name': 'Cotton', 'code': 'CTN'},
            '3': {'name': 'Corn', 'code': 'CRN'}, 
            '4': {'name': 'Soy', 'code': 'SOY'}
        }
        
        chains = chains or ['1', '3', '4']  # All crops
        all_data = []
        
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        for chain_id in chains:
            try:
                endpoint = f'/api/v2/mobile/cadeias/{chain_id}/cotacoes'
                self.logger.info(f"📊 Fetching {chain_mapping.get(chain_id, {}).get('name', f'Chain {chain_id}')} prices...")
                
                response = self.session.get(
                    f'{self.base_url}{endpoint}',
                    headers=headers,
                    verify=False
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data:
                        # Add chain information
                        for record in data:
                            record['CadeiaId'] = chain_id
                            record['CadeiaNome'] = chain_mapping.get(chain_id, {}).get('name', f'Chain {chain_id}')
                        
                        all_data.extend(data)
                        self.logger.info(f"✅ Retrieved {len(data)} price records for chain {chain_id}")
                    else:
                        self.logger.warning(f"⚠️ No price data for chain {chain_id}")
                        
                else:
                    self.logger.error(f"❌ Price request failed for chain {chain_id}: {response.status_code}")
                    
            except Exception as e:
                self.logger.error(f"❌ Error extracting prices for chain {chain_id}: {e}")
        
        if all_data:
            df = pd.DataFrame(all_data)
            self.logger.info(f"✅ Total current price records: {len(df)}")
            
            # Transform and clean data
            df = self._transform_price_data(df)
            return df
        else:
            self.logger.warning("⚠️ No price data retrieved")
            return None
    
    def _transform_historical_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean historical series data focusing on percentage metrics"""
        try:
            # Convert date column
            if 'Data' in df.columns:
                df['Data'] = pd.to_datetime(df['Data'])
                df['date'] = df['Data'].dt.date
                df['year'] = df['Data'].dt.year
                df['month'] = df['Data'].dt.month
            
            # Convert numeric values (these are percentages)
            if 'Valor' in df.columns:
                df['percentage'] = pd.to_numeric(df['Valor'], errors='coerce')
            
            # Use the metadata we added during extraction
            df['crop'] = df['_crop']
            df['activity'] = df['_activity']
            
            # Add state information
            if 'EstadoId' in df.columns:
                df['state'] = df['EstadoId'].map(lambda x: self.states.get(str(x), {}).get('name', f'State {x}'))
            else:
                df['state'] = 'Mato Grosso'  # Default assumption
            
            # Add harvest season info
            df['harvest_season'] = df.get('SafraDescricao', 'Unknown')
            
            # Filter only percentage data (unit should be 'Percentual')
            if 'UnidadeDescricao' in df.columns:
                df = df[df['UnidadeDescricao'] == 'Percentual'].copy()
            
            # Create final structure for CSV
            final_df = df[[
                'date', 'year', 'month', 'crop', 'activity', 'percentage', 
                'harvest_season', 'state'
            ]].copy()
            
            # Add extraction metadata
            final_df['extraction_date'] = datetime.now()
            final_df['data_source'] = 'IMEA_API_INDICATORS'
            
            self.logger.info(f"✅ Historical data transformed: {len(final_df)} percentage records")
            
            # Log breakdown by crop and activity
            breakdown = final_df.groupby(['crop', 'activity']).size().reset_index(name='count')
            self.logger.info(f"📊 Data breakdown by crop and activity:")
            for _, row in breakdown.iterrows():
                self.logger.info(f"   {row['crop']} {row['activity']}: {row['count']} records")
            
            return final_df
            
        except Exception as e:
            self.logger.error(f"❌ Error transforming historical data: {e}")
            return df
    
    def _transform_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean price data"""
        try:
            # Convert numeric values
            if 'Valor' in df.columns:
                df['value'] = pd.to_numeric(df['Valor'], errors='coerce')
            
            if 'Variacao' in df.columns:
                df['variation'] = pd.to_numeric(df['Variacao'], errors='coerce')
            
            # Convert date
            if 'DataPublicacao' in df.columns:
                df['publication_date'] = pd.to_datetime(df['DataPublicacao'])
            
            # Add extraction metadata
            df['extraction_date'] = datetime.now()
            df['source'] = 'IMEA_DIRECT_PRICES'
            
            # Reorder columns
            key_columns = [
                'Localidade', 'value', 'variation', 'CadeiaNome', 'Safra',
                'UnidadeDescricao', 'publication_date', 'extraction_date', 'source'
            ]
            
            # Add remaining columns
            remaining_columns = [col for col in df.columns if col not in key_columns]
            df = df[key_columns + remaining_columns]
            
            self.logger.info("✅ Price data transformed successfully")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Error transforming price data: {e}")
            return df
    
    def extract(self, **kwargs) -> Dict[str, Any]:
        """
        Main extraction method
        
        Args:
            **kwargs: Optional parameters for customization
                - extract_historical: bool (default True)
                - extract_prices: bool (default True)
                - start_date: str (YYYY-MM-DD format)
                - end_date: str (YYYY-MM-DD format)
        
        Returns:
            Dictionary with extraction results
        """
        self.logger.info(f"🚀 Starting {self.name}")
        
        # Authenticate
        if not self.authenticate():
            return {
                'success': False,
                'error': 'Authentication failed',
                'data': {}
            }
        
        results = {
            'success': True,
            'extraction_time': datetime.now(),
            'data': {}
        }
        
        # Extract historical series using specific indicators
        if kwargs.get('extract_historical', True):
            historical_df = self.extract_historical_series(
                start_date=kwargs.get('start_date'),
                end_date=kwargs.get('end_date')
            )
            
            if historical_df is not None and not historical_df.empty:
                # Create the percentage summary
                percentage_summary = self.create_percentage_summary(historical_df)
                results['data']['percentage_summary'] = percentage_summary
                results['data']['detailed_data'] = historical_df
                self.logger.info(f"📈 Percentage data: {len(percentage_summary)} summary records, {len(historical_df)} detailed records")
            else:
                results['data']['percentage_summary'] = pd.DataFrame()
                results['data']['detailed_data'] = pd.DataFrame()
                self.logger.warning("⚠️ No historical percentage data extracted")
        
        # Extract current prices
        if kwargs.get('extract_prices', True):
            prices_df = self.extract_current_prices(
                chains=['1', '3', '4']  # All crops
            )
            
            if prices_df is not None:
                results['data']['current_prices'] = prices_df
                self.logger.info(f"💰 Current prices: {len(prices_df)} records")
            else:
                results['data']['current_prices'] = pd.DataFrame()
                self.logger.warning("⚠️ No current prices data extracted")
        
        # Summary
        total_records = sum(len(df) for df in results['data'].values() if isinstance(df, pd.DataFrame))
        self.logger.info(f"🎉 Extraction completed: {total_records} total records")
        
        return results

    def save_separated_files(self, df: pd.DataFrame) -> None:
        """
        Save extracted data as separate CSV files for each crop and activity combination.
        
        Args:
            df: DataFrame to save
        """
        try:
            crops = ['Soy', 'Corn', 'Cotton']
            activities = [
                ('Planting', 'planted_percentage', 'PLANTING'),
                ('Harvest', 'harvested_percentage', 'HARVEST'), 
                ('Commercialization', 'commercialized_percentage', 'COMMERCIALIZATION')
            ]
            
            saved_files = []
            total_records = 0
            
            for crop in crops:
                # Filter data for this crop
                crop_data = df[df['crop'] == crop].copy()
                
                if crop_data.empty:
                    self.logger.warning(f"⚠️ No data for {crop}")
                    continue
                
                for activity_name, percentage_col, file_suffix in activities:
                    # Check if percentage column exists (it should in the pivoted summary)
                    if percentage_col not in crop_data.columns:
                        self.logger.warning(f"⚠️ Column {percentage_col} not found for {crop} {activity_name}")
                        continue
                    
                    # Filter for records with non-zero values for this activity
                    activity_data = crop_data[crop_data[percentage_col] > 0].copy()
                    
                    if activity_data.empty:
                        self.logger.warning(f"⚠️ No {activity_name.lower()} data for {crop}")
                        continue
                    
                    # Create clean dataset for this crop-activity combination
                    clean_data = activity_data[[
                        'date', 'year', 'month', 'crop', 'state', 'harvest_season', percentage_col
                    ]].copy()
                    
                    # Rename the percentage column to just 'percentage' for clarity
                    clean_data = clean_data.rename(columns={percentage_col: 'percentage'})
                    
                    # Sort by date
                    clean_data = clean_data.sort_values('date').reset_index(drop=True)
                    
                    # Create filename
                    crop_upper = crop.upper()
                    filename = f'BR_IMEA_{crop_upper}_{file_suffix}_PERCENTAGE.csv'
                    
                    # Save the file
                    file_path = save_dataset(clean_data, filename, index=False)
                    saved_files.append((filename, len(clean_data), file_path))
                    total_records += len(clean_data)
                    
                    self.logger.info(f"📊 Saved {crop} {activity_name}: {len(clean_data)} records to {filename}")
            
            # Print summary
            self.logger.info(f"\n📋 SEPARATED FILES SUMMARY:")
            self.logger.info(f"Total files created: {len(saved_files)}")
            self.logger.info(f"Total records across all files: {total_records:,}")
            if not df.empty:
                self.logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
                self.logger.info(f"Harvest seasons: {sorted(df['harvest_season'].unique())}")
            
            # List all saved files
            self.logger.info(f"\n📁 Files created:")
            for filename, record_count, file_path in saved_files:
                self.logger.info(f"  {filename}: {record_count:,} records")
            
        except Exception as e:
            raise ValueError(f"Failed to save separated files: {str(e)}")
    
    def save(self, df: pd.DataFrame) -> None:
        """
        Save extracted data to CSV files - only creates separate files for each crop/activity.
        
        Args:
            df: DataFrame to save
        """
        # Save separated files instead of one consolidated file
        self.save_separated_files(df)

def run():
    """Entry point for the extractor following the standard pattern"""
    try:
        # Setup logging for the run function
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Credentials (in production, use environment variables)
        credentials = {
            'username': 'gpalhares@santander.com.br',
            'password': 'Santander24@@'
        }
        
        # Initialize extractor
        extractor = IMEADirectExtractor(credentials)
        
        # Extract all available data using the new indicator-based approach
        results = extractor.extract(
            extract_historical=True,
            extract_prices=False,
            start_date='',  # No start date = get all available data
            end_date='',    # No end date = get all available data
        )
        
        if results['success'] and 'percentage_summary' in results['data']:
            df = results['data']['percentage_summary']
            if not df.empty:
                extractor.save(df)
                return df
            else:
                raise ValueError("No percentage data extracted")
        else:
            raise ValueError(f"Extraction failed: {results.get('error', 'Unknown error')}")
            
    except Exception as e:
        logging.error(f"Error running IMEA Direct extractor: {str(e)}")
        raise

def main():
    """Example usage of the IMEA Direct Extractor"""
    import os
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Credentials (in production, use environment variables)
    credentials = {
        'username': 'gpalhares@santander.com.br',
        'password': 'Santander24@@'
    }
    
    # Initialize extractor
    extractor = IMEADirectExtractor(credentials)
    
    # Example extraction with all available data using specific indicators
    results = extractor.extract(
        extract_historical=True,
        extract_prices=False,  # Focus on percentage data
        start_date='',  # Get all available data
        end_date='',    # Get all available data
    )
    
    if results['success']:
        # Save the main percentage summary CSV using the standard pattern
        if 'percentage_summary' in results['data'] and not results['data']['percentage_summary'].empty:
            df = results['data']['percentage_summary']
            
            # Follow the naming convention from other extractors
            datasets_dir = ensure_datasets_dir()
            file_path = save_dataset(df, extractor.filename, index=False)
            print(f"📊 Saved crop percentage data: {len(df)} records to {file_path}")
            
            # Show sample of the data
            print("\nSample of percentage data:")
            print(df.head())
            
            # Show data summary
            print(f"\n📈 Data Summary:")
            print(f"Date range: {df['date'].min()} to {df['date'].max()}")
            print(f"Total records: {len(df):,}")
            print("Records by crop:")
            print(df['crop'].value_counts())
            print(f"Non-zero planted: {(df['planted_percentage'] > 0).sum()}")
            print(f"Non-zero harvested: {(df['harvested_percentage'] > 0).sum()}")
            print(f"Non-zero commercialized: {(df['commercialized_percentage'] > 0).sum()}")
        
        # Save detailed data if needed (optional backup)
        if 'detailed_data' in results['data'] and not results['data']['detailed_data'].empty:
            df = results['data']['detailed_data'] 
            detailed_filename = 'BR_IMEA_CROP_PERCENTAGE_DETAILED.csv'
            file_path = save_dataset(df, detailed_filename, index=False)
            print(f"📋 Saved detailed data: {len(df)} records to {file_path}")
    else:
        print(f"❌ Extraction failed: {results.get('error', 'Unknown error')}")

if __name__ == "__main__":
    run() 