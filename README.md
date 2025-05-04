# cadastral-scraper (scraperdb.py)

A high-performance asynchronous scraper for collecting cadastral (land registry) data from the Croatian land registry system oss.uredjenazemlja.hr. This scraper efficiently collects and stores detailed information about land parcels including ownership details, area, and location data, as well as comprehensive land registry ("Zemljoknjizni podaci") information.

## Features

- **Asynchronous Processing**: Built with `aiohttp` for high-throughput concurrent HTTP requests
- **Persistent Storage**: SQLite database integration for reliable data storage
- **Fault Tolerance**: Robust error handling and automatic retry mechanism
- **Real-time Monitoring**: Live progress tracking with ETA and success rates
- **Rate Limiting**: Configurable request throttling to prevent overload
- **Batch Processing**: Efficient processing of parcels in large batches
- **Auto-Resume**: Capable of continuing from last processed ID if interrupted
- **Memory Efficient**: Streams data directly to database without excessive memory usage

## Data Collection

The scraper now collects comprehensive information for each parcel, including:

### Main Parcel Information
- Parcel ID and number
- Municipality details (ID, registration number, name)
- Institution ID
- Address and area
- Building remarks and detail sheet number
- Building rights and legal status
- Resource and status information

### Parcel Parts
- Part ID and name
- Type and area
- Possession sheet information
- Building status
- Change log information

### Ownership Information
- Multiple possessors/owners for each parcel
- Ownership share percentages
- Owner addresses

### Land Registry Units
- Unit IDs and numbers
- Main book information
- Institution details
- Status and verification information
- Unit type information

### Detailed Land Registry Data ("Zemljoknjizni podaci")
- Complete ownership information from sheet B ("vlasnički list")
- Ownership shares and co-ownership details
- Owner names and addresses
- Parcel information from possession sheet A1
- Last diary number and verification status
- Raw JSON data for complete record preservation

### Parcel Links
- Linked parcel information
- Associated land registry units

## Prerequisites

```bash
pip install aiohttp sqlite3
```

## Usage

1. Clone the repository or download `scraperdb.py`

2. Basic usage with default settings:
```bash
python scraperdb.py
```

3. The script will create a SQLite database named `cadastral.db` in your working directory.

## Configuration

### Custom Database Name

```python
# In the script
scraper = FastCadastralScraper(db_name='custom.db')
```

### Modify Scraping Range

```python
# In the main() function
start_id = 5625555  # Custom start ID
end_id = 40150506  # Custom end ID
```

### Adjust Concurrency

```python
# In the FastCadastralScraper class
self.semaphore = asyncio.Semaphore(1000)  # Modify concurrent request limit
```

### Batch Size

```python
# In the FastCadastralScraper class
self.batch_size = 10000  # Modify batch size
```

## Output and Monitoring

The scraper provides detailed progress information during execution:

```
Progress: 100,000/40,150,506 (0.25%) | Records: 98,750 | ETA: 2:15:30
Success rate: 98.75% | Successful: 98,750 | Failed: 1,250
```

### Progress Metrics

- Current position in ID range
- Overall completion percentage
- Total records collected
- Estimated time remaining
- Success/failure statistics (updated every 10,000 successful requests)

## Database Schema

The database now uses a relational structure with multiple tables to store all available information:

### Main Tables

```sql
-- Main parcels table
CREATE TABLE parcels (
    parcel_id INTEGER PRIMARY KEY,
    parcel_number TEXT,
    cad_municipality_id INTEGER,
    cad_municipality_reg_num INTEGER,
    cad_municipality_name TEXT,
    institution_id INTEGER,
    address TEXT,
    area REAL,
    building_remark TEXT,
    detail_sheet_number TEXT,
    has_building_right BOOLEAN,
    is_additional_data_set BOOLEAN,
    legal_regime BOOLEAN,
    graphic BOOLEAN,
    alpha_numeric BOOLEAN,
    status INTEGER,
    resource_code INTEGER,
    is_harmonized BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

-- Parcel parts table
CREATE TABLE parcel_parts (
    parcel_part_id INTEGER PRIMARY KEY,
    parcel_id INTEGER,
    name TEXT,
    type TEXT,
    area REAL,
    possession_sheet_id INTEGER,
    possession_sheet_number TEXT,
    building_right INTEGER,
    building BOOLEAN,
    last_change_log_file_num TEXT,
    last_change_log_number TEXT,
    FOREIGN KEY (parcel_id) REFERENCES parcels (parcel_id) ON DELETE CASCADE
)

-- Possession sheets table
CREATE TABLE possession_sheets (
    possession_sheet_id INTEGER PRIMARY KEY,
    possession_sheet_number TEXT,
    cad_municipality_id INTEGER,
    cad_municipality_reg_num INTEGER,
    cad_municipality_name TEXT,
    possession_sheet_type_id INTEGER,
    parcel_id INTEGER,
    FOREIGN KEY (parcel_id) REFERENCES parcels (parcel_id) ON DELETE CASCADE
)

-- Possessors table
CREATE TABLE possessors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    possession_sheet_id INTEGER,
    name TEXT,
    ownership TEXT,
    address TEXT,
    FOREIGN KEY (possession_sheet_id) REFERENCES possession_sheets (possession_sheet_id) ON DELETE CASCADE
)

-- Parcel links table
CREATE TABLE parcel_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parcel_id INTEGER,
    linked_parcel_id INTEGER,
    parcel_number TEXT,
    address TEXT,
    area REAL,
    FOREIGN KEY (parcel_id) REFERENCES parcels (parcel_id) ON DELETE CASCADE
)

-- Land registry units table
CREATE TABLE lr_units (
    lr_unit_id INTEGER PRIMARY KEY,
    lr_unit_number TEXT,
    main_book_id INTEGER,
    main_book_name TEXT,
    cadastre_municipality_id INTEGER,
    institution_id INTEGER,
    institution_name TEXT,
    status INTEGER,
    status_name TEXT,
    verificated BOOLEAN,
    condominiums BOOLEAN,
    lr_unit_type_id INTEGER,
    lr_unit_type_name TEXT,
    parcel_id INTEGER,
    FOREIGN KEY (parcel_id) REFERENCES parcels (parcel_id) ON DELETE CASCADE
)

-- Land registry detailed data tables

-- Land registry unit details table
CREATE TABLE lr_unit_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lr_unit_id INTEGER,
    lr_unit_number TEXT,
    main_book_id INTEGER,
    main_book_name TEXT,
    cadastre_municipality_id INTEGER,
    institution_id INTEGER,
    institution_name TEXT,
    status TEXT,
    status_name TEXT,
    verificated BOOLEAN,
    condominiums BOOLEAN,
    lr_unit_type_id INTEGER,
    lr_unit_type_name TEXT,
    last_diary_number TEXT,
    raw_data TEXT,
    parcel_id INTEGER,
    FOREIGN KEY (parcel_id) REFERENCES parcels (parcel_id) ON DELETE CASCADE
)

-- Land registry ownership sheet B table
CREATE TABLE lr_ownership_shares (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lr_unit_id INTEGER,
    lr_unit_share_id INTEGER,
    description TEXT,
    order_number TEXT,
    status INTEGER,
    FOREIGN KEY (lr_unit_id) REFERENCES lr_unit_details (lr_unit_id) ON DELETE CASCADE
)

-- Land registry owners table
CREATE TABLE lr_owners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lr_owner_id INTEGER,
    lr_unit_share_id INTEGER,
    name TEXT,
    address TEXT,
    FOREIGN KEY (lr_unit_share_id) REFERENCES lr_ownership_shares (lr_unit_share_id) ON DELETE CASCADE
)

-- Land registry parcels table (from possession sheet A1)
CREATE TABLE lr_parcels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lr_unit_id INTEGER,
    parcel_id INTEGER,
    parcel_number TEXT,
    address TEXT,
    area TEXT,
    status_in_lr_unit INTEGER,
    FOREIGN KEY (lr_unit_id) REFERENCES lr_unit_details (lr_unit_id) ON DELETE CASCADE
)
```

## Land Registry Data Integration

The scraper now fetches detailed land registry data ("Zemljoknjizni podaci") from a dedicated API endpoint:

```
https://oss.uredjenazemlja.hr/oss/public/lr/lr-unit
```

### How It Works

1. **Endpoint Parameters**: The API requires two key parameters:
   - `lrUnitNumber`: The land registry unit number
   - `mainBookId`: The main book ID
   - `historicalOverview`: Set to "false" to get current data only

2. **Integration Process**:
   - For each parcel, the scraper checks if there are any associated land registry units
   - When a land registry unit is found, it extracts the unit number and main book ID
   - It then makes an additional API request to fetch the detailed land registry data
   - The response is parsed and stored in the database in a structured format
   - The complete raw JSON response is also stored for future reference

3. **Data Structure**:
   - The land registry data is organized into multiple related tables
   - This preserves the hierarchical structure of the original data
   - Relationships between entities (units, ownership shares, owners, parcels) are maintained
   - Indexes are created for efficient querying

### Example API Request

```
https://oss.uredjenazemlja.hr/oss/public/lr/lr-unit?lrUnitNumber=213&mainBookId=30667&historicalOverview=false
```

## Performance Optimization

The scraper implements several performance optimizations:

- Connection pooling for efficient resource usage
- Keep-alive connections to reduce overhead
- Batch processing to minimize database operations
- Indexed database fields for quick lookups
- Asynchronous I/O for maximum throughput

## Error Handling

- Automatic retry mechanism for failed requests
- Graceful handling of network timeouts
- Database transaction management
- Comprehensive error logging

## Limitations

- Designed specifically for the Croatian cadastral system (uredjenazemlja.hr)
- Performance dependent on API rate limits and response times

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Disclaimer

This scraper is for educational purposes only. Please ensure you have permission to access and store the data, and comply with all relevant terms of service and data protection regulations.

---

**Note**: Remember to review and comply with the terms of service of the target website before using this scraper.

![meme](https://i.redd.it/o6xypg00uac91.png "meme")
