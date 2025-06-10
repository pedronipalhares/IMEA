# IMEA Direct Data Extractor 🌾📈

A simple Python tool for extracting real-time agricultural data from IMEA (Instituto Mato-grossense de Economia Agropecuária) API, providing crucial insights into Brazil's agricultural sector for equity analysts, traders, and researchers.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Data Source: IMEA](https://img.shields.io/badge/Data%20Source-IMEA-green.svg)](https://www.imea.com.br/)

## 🎯 Why This Matters for Equity Analysts

### Critical Market Intelligence for Agricultural Commodities

Brazil is the world's largest exporter of soybeans and a major producer of corn and cotton. Mato Grosso alone accounts for:
- **32%** of Brazil's soybean production
- **28%** of Brazil's corn production  
- **65%** of Brazil's cotton production

This data extractor provides **real-time crop progress data** that directly impacts:

#### 📊 **Commodity Price Movements**
- **Planting Progress**: Early indicators of potential supply
- **Harvest Progress**: Real-time production estimates
- **Commercialization Progress**: Market flow and pricing pressure

#### 🏢 **Equity Impact Analysis**
- **Agricultural Companies**: ADM, Cargill, Bunge, Amaggi
- **Equipment Manufacturers**: John Deere, CNH Industrial, AGCO
- **Fertilizer Companies**: Nutrien, Mosaic, Yara
- **Food & Beverage**: Tyson Foods, JBS, BRF
- **Biofuel Producers**: Renewable Energy Group, Archer Daniels

## 🚀 Quick Start (5 minutes)

### 1. Clone and Install
```bash
git clone https://github.com/yourusername/imea-direct-extractor.git
cd imea-direct-extractor
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Set Your Credentials
Create a `.env` file:
```bash
IMEA_USERNAME=your_email@example.com
IMEA_PASSWORD=your_password
```

### 3. Extract Data
```bash
python imea_extractor.py
```

That's it! You'll get 10 CSV files with all the agricultural data.

## 📊 What You Get

### Main Summary File
- `BR_IMEA_CROP_PERCENTAGE_PROGRESS.csv` - All crops and activities combined

### Individual Crop Files (9 files)
- `BR_IMEA_SOY_PLANTING_PERCENTAGE.csv`
- `BR_IMEA_SOY_HARVEST_PERCENTAGE.csv`
- `BR_IMEA_SOY_COMMERCIALIZATION_PERCENTAGE.csv`
- `BR_IMEA_CORN_PLANTING_PERCENTAGE.csv`
- `BR_IMEA_CORN_HARVEST_PERCENTAGE.csv`
- `BR_IMEA_CORN_COMMERCIALIZATION_PERCENTAGE.csv`
- `BR_IMEA_COTTON_PLANTING_PERCENTAGE.csv`
- `BR_IMEA_COTTON_HARVEST_PERCENTAGE.csv`
- `BR_IMEA_COTTON_COMMERCIALIZATION_PERCENTAGE.csv`

### Data Structure
```csv
date,year,month,crop,state,harvest_season,planted_percentage,harvested_percentage,commercialized_percentage
2024-01-15,2024,1,Soy,Mato Grosso,Safra 2023/24,98.5,15.2,75.8
```

## 📈 Real-World Applications

1. **Earnings Forecasting**: Predict quarterly results for agribusiness companies
2. **Risk Management**: Hedge positions based on crop progress data
3. **Sector Rotation**: Time rotation into/out of agricultural stocks
4. **Weather Impact**: Correlate planting delays with price volatility
5. **Supply Chain Analysis**: Track commercialization for logistics companies

## 🔧 Technical Features

- ✅ **Direct API Access**: No email workflows, real-time data
- ✅ **Comprehensive Coverage**: Soy, Corn, and Cotton from 2021+
- ✅ **Clean CSV Output**: Ready for Excel, Python, R analysis
- ✅ **Parallel Processing**: Fast bulk data extraction
- ✅ **Robust Authentication**: Handles IMEA's SSL requirements
- ✅ **Simple Setup**: One file, minimal dependencies

## 📊 Sample Data Analysis

```python
import pandas as pd

# Load main summary
df = pd.read_csv('datasets/BR_IMEA_CROP_PERCENTAGE_PROGRESS.csv')

# Latest crop status
latest = df[df['date'] == df['date'].max()]
print("Current Crop Progress:")
for crop in ['Soy', 'Corn', 'Cotton']:
    crop_data = latest[latest['crop'] == crop]
    if not crop_data.empty:
        row = crop_data.iloc[0]
        print(f"{crop}: {row['planted_percentage']:.1f}% planted, {row['harvested_percentage']:.1f}% harvested")

# Seasonal trends
import matplotlib.pyplot as plt
soy_data = df[df['crop'] == 'Soy']
plt.plot(soy_data['date'], soy_data['planted_percentage'], label='Soy Planting')
plt.title('Soy Planting Progress Over Time')
plt.show()
```

## 🏗️ File Structure

```
imea-direct-extractor/
├── imea_extractor.py     # Main extractor (run this!)
├── requirements.txt      # Dependencies
├── .env                 # Your credentials (create this)
├── datasets/            # Output CSV files (auto-created)
├── README.md           # This file
├── LICENSE             # MIT License
└── .gitignore          # Git ignore (.env protected)
```

## 🔒 Security

- ✅ Credentials stored in `.env` file (git-ignored)
- ✅ No hardcoded passwords in code
- ✅ SSL warnings handled appropriately
- ✅ Environment variable loading

## 🤝 Contributing

We welcome contributions! Key areas:
- Additional Brazilian states
- More crop types (wheat, coffee, sugarcane)
- Data visualization features
- Performance optimizations

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## ⚠️ Disclaimer

This tool is for informational purposes only. Users are responsible for:
- Complying with IMEA's Terms of Service
- Ensuring proper API usage
- Validating data accuracy
- Making independent investment decisions

## 🆘 Support

- 🐛 **Issues**: [GitHub Issues](https://github.com/yourusername/imea-direct-extractor/issues)
- 📧 **Email**: support@yourproject.com

---

**Made with ❤️ for the agricultural finance community**

*"Transforming agricultural data into actionable insights for better investment decisions."* 