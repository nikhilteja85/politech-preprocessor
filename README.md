# Politech Processor - Multi-State Political Data Analysis Pipeline

A comprehensive Python pipeline for processing political and demographic data for any US state. This toolkit downloads Census/ACS data, processes precinct-level demographics, handles redistricting plans, generates dot maps, and creates comparative visualizations.

## 🚀 Quick Start

### 1. Setup Environment
```bash
# Clone and navigate to the project
cd /path/to/politech-processor

# Create virtual environment with Python 3.11
/opt/homebrew/bin/python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Create a `.env` file in the project root:
```bash
# Required for Stage 0 (Census API data collection)
CENSUS_API_KEY=your_census_api_key_here
```
Get your Census API key from: https://api.census.gov/data/key_signup.html

### 3. Run for Any State
```bash
# Run all stages for Arizona
python scripts/run_all_stages.py AZ

# Run specific stages for California 
python scripts/run_all_stages.py CA --stages 0,1,2

# Skip data download if already exists
python scripts/run_all_stages.py TX --skip-stage0

# Use different parameters
python scripts/run_all_stages.py FL --acs-year 2022 --dot-unit 100
```

## 📊 Pipeline Stages

### Stage 0: Data Collection (`run_stage0.py`)
**Downloads TIGER shapefiles and ACS demographic data**
```bash
python run_stage0.py <STATE_CODE> [--acs-year YEAR] [--census-year YEAR]
```
- Downloads block group and tabulation block shapefiles
- Fetches ACS 5-year race and income data at block group level
- **Outputs**: `inputs/tiger_2020/`, `inputs/acs_2023/<state>/`

### Stage 1: Demographic Processing (`run_stage1.py`)  
**Aggregates demographics from block groups to precincts**
```bash
python run_stage1.py <STATE_CODE> [--acs-year YEAR]
```
- Merges ACS race, CVAP, and income data with block group geometries
- Uses `maup` library to aggregate data to precinct level
- **Outputs**: `outputs/<state>/<state>_precinct_all_pop_<year>.geojson`

### Stage 2: Plan Processing (`run_stage2.py`)
**Processes redistricting plans and creates assignments**  
```bash
python run_stage2.py <STATE_CODE> [--plan-year YEAR]
```
- Loads congressional and legislative district plans
- Assigns precincts to districts using spatial intersection
- **Outputs**: `outputs/<state>/<state>_plans_<year>.json`, `assignments.json`

### Stage 3: Dot Map Generation (`run_stage3_dots.py`)
**Creates race dot maps for visualization**
```bash
python run_stage3_dots.py <STATE_CODE> [--dot-unit PEOPLE_PER_DOT] [--acs-year YEAR]
```
- Generates dot density maps with configurable people per dot
- Creates separate files for each racial/ethnic group
- **Outputs**: `outputs/<state>/<state>_dots_pop<year>_unit<X>.geojson`

### Stage 4: Visualization (`run_stage4_comp.py`)
**Creates comparative visualizations**
```bash
python run_stage4_comp.py <STATE_CODE> [--dot-unit PEOPLE_PER_DOT] [--plan-year YEAR]
```
- Generates side-by-side maps comparing congressional vs legislative districts
- Overlays demographic dots, precinct boundaries, and block group boundaries
- **Outputs**: Interactive matplotlib visualizations

## 📁 Project Structure

```
politech-processor/
├── config/
│   └── states.json              # All US state codes and FIPS codes
├── scripts/
│   ├── common.py                # Shared utilities and configuration
│   ├── run_all_stages.py        # Master script for running all stages
│   ├── run_stage0.py            # Data collection
│   ├── run_stage1.py            # Demographic processing  
│   ├── run_stage2.py            # Plan processing
│   ├── run_stage3_dots.py       # Dot map generation
│   └── run_stage4_comp.py       # Visualization
├── inputs/                      # Downloaded/input data (auto-created)
│   ├── tiger_2020/             # TIGER shapefiles by state
│   ├── acs_2023/               # ACS demographic data by state
│   ├── cvap/                   # CVAP data files
│   ├── precincts/              # Precinct shapefiles by state (user-provided)
│   └── plans/                  # District plan shapefiles by state (user-provided)
├── outputs/                    # Generated outputs (auto-created)
│   └── <state>/               # State-specific outputs
│       ├── <state>_bg_all_data_<year>.geojson
│       ├── <state>_precinct_all_pop_<year>.geojson
│       ├── <state>_dots_pop<year>_unit<X>.geojson
│       └── comparison CSVs and JSON files
├── requirements.txt            # Python dependencies
├── .env                       # Environment variables (create this)
└── README.md                  # This file
```

## 🗺️ Supported States

All 50 US states plus DC are supported. Use standard two-letter abbreviations:

**Examples**: `AZ`, `CA`, `TX`, `FL`, `NY`, `PA`, `IL`, `OH`, `GA`, `NC`, `MI`, `NJ`, `VA`, `WA`, `MA`, etc.

## 📋 Data Requirements

### Automatically Downloaded (Stage 0):
- **TIGER Shapefiles**: Block groups and tabulation blocks from Census Bureau
- **ACS Data**: 5-year American Community Survey race and income data
- **CVAP Data**: Citizen Voting Age Population data

### User-Provided Data:
Place these in the appropriate directories before running later stages:

1. **Precinct Shapefiles** → `inputs/precincts/<state>/`
   - Election precinct boundaries with voting results
   - Available from Redistricting Data Hub or state election offices

2. **District Plan Shapefiles** → `inputs/plans/<state>/`
   - Congressional and legislative district boundaries
   - Format: `<state>_cong_adopted_<year>/` and `<state>_sl_adopted_<year>/`
   - Available from Redistricting Data Hub

## 🔧 Configuration Options

### Command Line Arguments:
- `--acs-year`: ACS data year (default: 2023)
- `--census-year`: Census/TIGER year (default: 2020) 
- `--dot-unit`: People per dot for dot maps (default: 50)
- `--plan-year`: Redistricting plan year (default: 2022)

### Environment Variables:
- `CENSUS_API_KEY`: Required for Census API access

## 🎯 Use Cases

### Political Analysis:
- **Redistricting Analysis**: Compare district compactness and demographic composition
- **Voting Pattern Analysis**: Analyze precinct-level election results with demographics
- **Population Distribution**: Visualize racial and ethnic population patterns

### Academic Research:
- **Electoral Geography**: Study relationships between demographics and voting
- **Urban Planning**: Analyze population density and distribution patterns  
- **Civil Rights**: Examine voting rights and representation issues

### Data Journalism:
- **Election Coverage**: Create compelling demographic and electoral visualizations
- **Redistricting Stories**: Illustrate the impact of district boundary changes
- **Census Analysis**: Report on demographic trends and changes

## 🛠️ Advanced Usage

### Running Individual Stages:
```bash
# Download data for multiple states
python run_stage0.py AZ
python run_stage0.py CA  
python run_stage0.py TX

# Process demographics with custom year
python run_stage1.py FL --acs-year 2022

# Generate high-resolution dot maps
python run_stage3_dots.py NY --dot-unit 25

# Create visualizations for older plans
python run_stage4_comp.py PA --plan-year 2018
```

### Batch Processing Multiple States:
```bash
# Create a simple batch script
for state in AZ CA TX FL NY; do
  echo "Processing $state..."
  python run_all_stages.py $state --skip-stage0
done
```

### Custom Analysis:
The generated GeoJSON and CSV files can be loaded into:
- **QGIS/ArcGIS**: For advanced spatial analysis
- **Python/R**: For statistical analysis and custom visualizations  
- **Tableau/Power BI**: For interactive dashboards
- **Web mapping**: Leaflet, Mapbox, or similar platforms

## 📊 Output Data Formats

### GeoJSON Files:
- **Geometric**: All spatial data with CRS EPSG:4326 (WGS84)
- **Demographic**: Population and CVAP by race/ethnicity  
- **Geographic**: GEOID identifiers for joining with other datasets

### CSV Files:
- **Comparison**: Data quality metrics for aggregation processes
- **Validation**: Population totals and differences between geographic levels

### JSON Files:
- **Plans**: Redistricting plan metadata and district information
- **Assignments**: Precinct-to-district assignment mappings

## 🔍 Troubleshooting

### Common Issues:

**Import errors**: Make sure you're using Python 3.11 and have installed all requirements:
```bash
python --version  # Should show 3.11.x
pip install -r requirements.txt
```

**Missing Census API key**:
```bash
echo "CENSUS_API_KEY=your_key_here" > .env
```

**Missing precinct/plan data**: Download from [Redistricting Data Hub](https://redistrictingdatahub.org/)

**Memory issues with large states**: Use higher `--dot-unit` values (100-200) for initial testing

### Performance Tips:
- **Stage 0**: Can take 5-15 minutes per state depending on size
- **Stage 1**: Memory intensive for large states (CA, TX, FL)
- **Stage 3**: Dot generation time scales with population and dot density
- **Stage 4**: Visualization rendering may be slow for dense dot maps

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- **Census Bureau**: For providing comprehensive demographic and geographic data
- **Redistricting Data Hub**: For standardized election and redistricting data
- **maup**: For the excellent geographic data aggregation library
- **GeoPandas/Pandas**: For powerful geospatial and data analysis capabilities