# cadastral-scraper (scraperdb.py)

A high-performance asynchronous scraper for collecting cadastral (land registry) data from the Croatian land registry system oss.uredjenazemlja.hr. This scraper efficiently collects and stores detailed information about land parcels including ownership details, area, and location data.

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

The scraper collects the following information for each parcel:

- Parcel ID
- Municipality name
- Parcel number
- Address
- Area
- Owner information:
  - Name
  - Ownership type
  - Address

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

```sql
CREATE TABLE parcels (
    parcel_id INTEGER PRIMARY KEY,
    municipality_name TEXT,
    parcel_number TEXT,
    address TEXT,
    area REAL,
    owner_name TEXT,
    owner_ownership TEXT,
    owner_address TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
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
