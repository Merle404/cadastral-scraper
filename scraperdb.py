import aiohttp
import asyncio
import json
import sqlite3
from typing import Dict, Optional, List, Any
import time
from datetime import datetime, timedelta
import os

class CadastralDatabase:
    def __init__(self, db_name='cadastral.db'):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.create_tables()
        
    def create_tables(self):
        with self.conn:
            # Main parcels table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS parcels (
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
            ''')
            
            # Parcel parts table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS parcel_parts (
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
            ''')
            
            # Possession sheets table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS possession_sheets (
                    possession_sheet_id INTEGER PRIMARY KEY,
                    possession_sheet_number TEXT,
                    cad_municipality_id INTEGER,
                    cad_municipality_reg_num INTEGER,
                    cad_municipality_name TEXT,
                    possession_sheet_type_id INTEGER,
                    parcel_id INTEGER,
                    FOREIGN KEY (parcel_id) REFERENCES parcels (parcel_id) ON DELETE CASCADE
                )
            ''')
            
            # Possessors table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS possessors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    possession_sheet_id INTEGER,
                    name TEXT,
                    ownership TEXT,
                    address TEXT,
                    FOREIGN KEY (possession_sheet_id) REFERENCES possession_sheets (possession_sheet_id) ON DELETE CASCADE
                )
            ''')
            
            # Parcel links table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS parcel_links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parcel_id INTEGER,
                    linked_parcel_id INTEGER,
                    parcel_number TEXT,
                    address TEXT,
                    area REAL,
                    FOREIGN KEY (parcel_id) REFERENCES parcels (parcel_id) ON DELETE CASCADE
                )
            ''')
            
            # Land registry units table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS lr_units (
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
            ''')
            
            # Land registry detailed data tables
            
            # Land registry unit details table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS lr_unit_details (
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
            ''')
            
            # Land registry ownership sheet B table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS lr_ownership_shares (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lr_unit_id INTEGER,
                    lr_unit_share_id INTEGER,
                    description TEXT,
                    order_number TEXT,
                    status INTEGER,
                    FOREIGN KEY (lr_unit_id) REFERENCES lr_unit_details (lr_unit_id) ON DELETE CASCADE
                )
            ''')
            
            # Land registry owners table
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS lr_owners (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lr_owner_id INTEGER,
                    lr_unit_share_id INTEGER,
                    name TEXT,
                    address TEXT,
                    FOREIGN KEY (lr_unit_share_id) REFERENCES lr_ownership_shares (lr_unit_share_id) ON DELETE CASCADE
                )
            ''')
            
            # Land registry parcels table (from possession sheet A1)
            self.conn.execute('''
                CREATE TABLE IF NOT EXISTS lr_parcels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lr_unit_id INTEGER,
                    parcel_id INTEGER,
                    parcel_number TEXT,
                    address TEXT,
                    area TEXT,
                    status_in_lr_unit INTEGER,
                    FOREIGN KEY (lr_unit_id) REFERENCES lr_unit_details (lr_unit_id) ON DELETE CASCADE
                )
            ''')
            
            # Create indexes for better performance
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_parcel_id ON parcels(parcel_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_parcel_part_parcel_id ON parcel_parts(parcel_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_possession_sheet_parcel_id ON possession_sheets(parcel_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_possessor_possession_sheet_id ON possessors(possession_sheet_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_parcel_link_parcel_id ON parcel_links(parcel_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_lr_unit_parcel_id ON lr_units(parcel_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_lr_unit_details_parcel_id ON lr_unit_details(parcel_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_lr_unit_details_lr_unit_id ON lr_unit_details(lr_unit_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_lr_ownership_shares_lr_unit_id ON lr_ownership_shares(lr_unit_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_lr_owners_lr_unit_share_id ON lr_owners(lr_unit_share_id)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_lr_parcels_lr_unit_id ON lr_parcels(lr_unit_id)')
    
    def insert_lr_unit_details(self, lr_unit_data: Dict, parcel_id: int):
        """Insert detailed land registry unit data into the database"""
        with self.conn:
            # First, insert the main LR unit details
            self.conn.execute('''
                INSERT OR REPLACE INTO lr_unit_details
                (lr_unit_id, lr_unit_number, main_book_id, main_book_name, 
                cadastre_municipality_id, institution_id, institution_name, status, 
                status_name, verificated, condominiums, lr_unit_type_id, 
                lr_unit_type_name, last_diary_number, raw_data, parcel_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                lr_unit_data.get('lrUnitId'),
                lr_unit_data.get('lrUnitNumber'),
                lr_unit_data.get('mainBookId'),
                lr_unit_data.get('mainBookName'),
                lr_unit_data.get('cadastreMunicipalityId'),
                lr_unit_data.get('institutionId'),
                lr_unit_data.get('institutionName'),
                lr_unit_data.get('status'),
                lr_unit_data.get('statusName'),
                lr_unit_data.get('verificated'),
                lr_unit_data.get('condominiums'),
                lr_unit_data.get('lrUnitTypeId'),
                lr_unit_data.get('lrUnitTypeName'),
                lr_unit_data.get('lastDiaryNumber'),
                json.dumps(lr_unit_data),
                parcel_id
            ))
            
            # Insert ownership sheet B data if available
            if 'ownershipSheetB' in lr_unit_data and lr_unit_data['ownershipSheetB']:
                ownership_sheet = lr_unit_data['ownershipSheetB']
                
                # Insert ownership shares
                if 'lrUnitShares' in ownership_sheet and ownership_sheet['lrUnitShares']:
                    for share in ownership_sheet['lrUnitShares']:
                        self.conn.execute('''
                            INSERT OR REPLACE INTO lr_ownership_shares
                            (lr_unit_id, lr_unit_share_id, description, order_number, status)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            lr_unit_data.get('lrUnitId'),
                            share.get('lrUnitShareId'),
                            share.get('description'),
                            share.get('orderNumber'),
                            share.get('status')
                        ))
                        
                        # Insert owners for this share
                        if 'lrOwners' in share and share['lrOwners']:
                            for owner in share['lrOwners']:
                                self.conn.execute('''
                                    INSERT OR REPLACE INTO lr_owners
                                    (lr_owner_id, lr_unit_share_id, name, address)
                                    VALUES (?, ?, ?, ?)
                                ''', (
                                    owner.get('lrOwnerId'),
                                    share.get('lrUnitShareId'),
                                    owner.get('name'),
                                    owner.get('address')
                                ))
            
            # Insert possession sheet A1 data if available
            if 'possessionSheetA1' in lr_unit_data and lr_unit_data['possessionSheetA1']:
                possession_sheet_a1 = lr_unit_data['possessionSheetA1']
                
                # Insert parcels
                if 'lrParcels' in possession_sheet_a1 and possession_sheet_a1['lrParcels']:
                    for parcel in possession_sheet_a1['lrParcels']:
                        self.conn.execute('''
                            INSERT OR REPLACE INTO lr_parcels
                            (lr_unit_id, parcel_id, parcel_number, address, area, status_in_lr_unit)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (
                            lr_unit_data.get('lrUnitId'),
                            parcel_id,
                            parcel.get('parcelNumber'),
                            parcel.get('address'),
                            parcel.get('area'),
                            parcel.get('statusInLrUnit')
                        ))
    
    def insert_parcel(self, parcel_data: Dict):
        with self.conn:
            # Insert main parcel data
            self.conn.execute('''
                INSERT OR REPLACE INTO parcels 
                (parcel_id, parcel_number, cad_municipality_id, cad_municipality_reg_num, 
                cad_municipality_name, institution_id, address, area, building_remark, 
                detail_sheet_number, has_building_right, is_additional_data_set, 
                legal_regime, graphic, alpha_numeric, status, resource_code, is_harmonized)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                parcel_data.get('parcel_id'),
                parcel_data.get('parcel_number'),
                parcel_data.get('cad_municipality_id'),
                parcel_data.get('cad_municipality_reg_num'),
                parcel_data.get('cad_municipality_name'),
                parcel_data.get('institution_id'),
                parcel_data.get('address'),
                parcel_data.get('area'),
                parcel_data.get('building_remark'),
                parcel_data.get('detail_sheet_number'),
                parcel_data.get('has_building_right'),
                parcel_data.get('is_additional_data_set'),
                parcel_data.get('legal_regime'),
                parcel_data.get('graphic'),
                parcel_data.get('alpha_numeric'),
                parcel_data.get('status'),
                parcel_data.get('resource_code'),
                parcel_data.get('is_harmonized')
            ))
            
            # Insert parcel parts
            for part in parcel_data.get('parcel_parts', []):
                self.conn.execute('''
                    INSERT OR REPLACE INTO parcel_parts
                    (parcel_part_id, parcel_id, name, type, area, possession_sheet_id, 
                    possession_sheet_number, building_right, building, last_change_log_file_num, 
                    last_change_log_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    part.get('parcel_part_id'),
                    parcel_data.get('parcel_id'),
                    part.get('name'),
                    part.get('type'),
                    part.get('area'),
                    part.get('possession_sheet_id'),
                    part.get('possession_sheet_number'),
                    part.get('building_right'),
                    part.get('building'),
                    part.get('last_change_log_file_num'),
                    part.get('last_change_log_number')
                ))
            
            # Insert possession sheets and possessors
            for sheet in parcel_data.get('possession_sheets', []):
                self.conn.execute('''
                    INSERT OR REPLACE INTO possession_sheets
                    (possession_sheet_id, possession_sheet_number, cad_municipality_id, 
                    cad_municipality_reg_num, cad_municipality_name, possession_sheet_type_id, parcel_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sheet.get('possession_sheet_id'),
                    sheet.get('possession_sheet_number'),
                    sheet.get('cad_municipality_id'),
                    sheet.get('cad_municipality_reg_num'),
                    sheet.get('cad_municipality_name'),
                    sheet.get('possession_sheet_type_id'),
                    parcel_data.get('parcel_id')
                ))
                
                # Insert possessors for this sheet
                for possessor in sheet.get('possessors', []):
                    self.conn.execute('''
                        INSERT OR REPLACE INTO possessors
                        (possession_sheet_id, name, ownership, address)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        sheet.get('possession_sheet_id'),
                        possessor.get('name'),
                        possessor.get('ownership'),
                        possessor.get('address')
                    ))
            
            # Insert parcel links
            for link in parcel_data.get('parcel_links', []):
                self.conn.execute('''
                    INSERT OR REPLACE INTO parcel_links
                    (parcel_id, linked_parcel_id, parcel_number, address, area)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    parcel_data.get('parcel_id'),
                    link.get('parcel_id'),
                    link.get('parcel_number'),
                    link.get('address'),
                    link.get('area')
                ))
                
                # Insert LR unit if available
                if 'lr_unit' in link:
                    lr_unit = link['lr_unit']
                    self.conn.execute('''
                        INSERT OR REPLACE INTO lr_units
                        (lr_unit_id, lr_unit_number, main_book_id, main_book_name, 
                        cadastre_municipality_id, status, verificated, condominiums, parcel_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        lr_unit.get('lr_unit_id'),
                        lr_unit.get('lr_unit_number'),
                        lr_unit.get('main_book_id'),
                        lr_unit.get('main_book_name'),
                        lr_unit.get('cadastre_municipality_id'),
                        lr_unit.get('status'),
                        lr_unit.get('verificated'),
                        lr_unit.get('condominiums'),
                        parcel_data.get('parcel_id')
                    ))
            
            # Insert LR units from parcel links
            for lr_unit in parcel_data.get('lr_units_from_parcel_links', []):
                self.conn.execute('''
                    INSERT OR REPLACE INTO lr_units
                    (lr_unit_id, lr_unit_number, main_book_id, main_book_name, 
                    cadastre_municipality_id, institution_id, institution_name, status, 
                    status_name, verificated, condominiums, lr_unit_type_id, lr_unit_type_name, parcel_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    lr_unit.get('lr_unit_id'),
                    lr_unit.get('lr_unit_number'),
                    lr_unit.get('main_book_id'),
                    lr_unit.get('main_book_name'),
                    lr_unit.get('cadastre_municipality_id'),
                    lr_unit.get('institution_id'),
                    lr_unit.get('institution_name'),
                    lr_unit.get('status'),
                    lr_unit.get('status_name'),
                    lr_unit.get('verificated'),
                    lr_unit.get('condominiums'),
                    lr_unit.get('lr_unit_type_id'),
                    lr_unit.get('lr_unit_type_name'),
                    parcel_data.get('parcel_id')
                ))
    
    def insert_many(self, records: List[Dict]):
        for record in records:
            self.insert_parcel(record)
    
    def get_last_id(self) -> Optional[int]:
        cursor = self.conn.execute('SELECT MAX(parcel_id) FROM parcels')
        return cursor.fetchone()[0]
    
    def get_stats(self) -> Dict:
        cursor = self.conn.execute('SELECT COUNT(*), MIN(parcel_id), MAX(parcel_id) FROM parcels')
        count, min_id, max_id = cursor.fetchone()
        return {'count': count, 'min_id': min_id, 'max_id': max_id}
    
    def close(self):
        self.conn.close()

class FastCadastralScraper:
    def __init__(self):
        self.base_url = "https://oss.uredjenazemlja.hr/oss/public/cad/parcel-info"
        self.lr_unit_url = "https://oss.uredjenazemlja.hr/oss/public/lr/lr-unit"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json'
        }
        self.batch_size = 10000
        self.db = CadastralDatabase()
        self.semaphore = asyncio.Semaphore(1000)
        self.start_time = time.time()
        self.successful_requests = 0
        self.failed_requests = 0

    async def get_lr_unit_details(self, session: aiohttp.ClientSession, lr_unit_number: str, main_book_id: int) -> Optional[Dict]:
        """Fetch detailed land registry unit data from the API"""
        async with self.semaphore:
            try:
                params = {
                    'lrUnitNumber': lr_unit_number,
                    'mainBookId': main_book_id,
                    'historicalOverview': 'false'
                }
                
                async with session.get(self.lr_unit_url, params=params) as response:
                    if response.status != 200:
                        self.failed_requests += 1
                        return None
                    
                    content = await response.json()
                    self.successful_requests += 1
                    
                    if not content or not isinstance(content, list) or len(content) == 0:
                        return None
                    
                    # Return the first item in the list (usually there's only one)
                    return content[0]
            except Exception as e:
                print(f"Error fetching LR unit details: {str(e)}")
                self.failed_requests += 1
                return None
    
    async def get_parcel_info(self, session: aiohttp.ClientSession, parcel_id: int) -> Optional[Dict]:
        async with self.semaphore:
            try:
                params = {'parcelId': parcel_id}
                async with session.get(self.base_url, params=params) as response:
                    if response.status != 200:
                        self.failed_requests += 1
                        return None
                    
                    content = await response.json()
                    self.successful_requests += 1
                    
                    if not content:
                        return None
                    
                    # Extract all data from the response
                    data = {
                        'parcel_id': content.get('parcelId'),
                        'parcel_number': content.get('parcelNumber'),
                        'cad_municipality_id': content.get('cadMunicipalityId'),
                        'cad_municipality_reg_num': content.get('cadMunicipalityRegNum'),
                        'cad_municipality_name': content.get('cadMunicipalityName'),
                        'institution_id': content.get('institutionId'),
                        'address': content.get('address'),
                        'area': content.get('area'),
                        'building_remark': content.get('buildingRemark'),
                        'detail_sheet_number': content.get('detailSheetNumber'),
                        'has_building_right': content.get('hasBuildingRight'),
                        'is_additional_data_set': content.get('isAdditionalDataSet'),
                        'legal_regime': content.get('legalRegime'),
                        'graphic': content.get('graphic'),
                        'alpha_numeric': content.get('alphaNumeric'),
                        'status': content.get('status'),
                        'resource_code': content.get('resourceCode'),
                        'is_harmonized': content.get('isHarmonized'),
                        'parcel_parts': [],
                        'possession_sheets': [],
                        'parcel_links': [],
                        'lr_units_from_parcel_links': []
                    }
                    
                    # Extract parcel parts
                    if 'parcelParts' in content and content['parcelParts']:
                        # Handle both direct array and nested 'parcelParts' structure
                        parcel_parts = content['parcelParts']
                        if isinstance(parcel_parts, dict) and 'parcelParts' in parcel_parts:
                            parcel_parts = parcel_parts['parcelParts']
                        
                        if isinstance(parcel_parts, list):
                            for part in parcel_parts:
                                data['parcel_parts'].append({
                                    'parcel_part_id': part.get('parcelPartId'),
                                    'name': part.get('name'),
                                    'type': part.get('type'),
                                    'area': part.get('area'),
                                    'possession_sheet_id': part.get('possessionSheetId'),
                                    'possession_sheet_number': part.get('possessionSheetNumber'),
                                    'building_right': part.get('buildingRight'),
                                    'building': part.get('building'),
                                    'last_change_log_file_num': part.get('lastChangeLogFileNum'),
                                    'last_change_log_number': part.get('lastChangeLogNumber')
                                })
                    
                    # Extract possession sheets
                    if 'possessionSheets' in content and content['possessionSheets']:
                        # Handle both direct array and nested 'possessionSheets' structure
                        possession_sheets = content['possessionSheets']
                        if isinstance(possession_sheets, dict) and 'possessionSheets' in possession_sheets:
                            possession_sheets = possession_sheets['possessionSheets']
                        
                        if isinstance(possession_sheets, list):
                            for sheet in possession_sheets:
                                sheet_data = {
                                    'possession_sheet_id': sheet.get('possessionSheetId'),
                                    'possession_sheet_number': sheet.get('possessionSheetNumber'),
                                    'cad_municipality_id': sheet.get('cadMunicipalityId'),
                                    'cad_municipality_reg_num': sheet.get('cadMunicipalityRegNum'),
                                    'cad_municipality_name': sheet.get('cadMunicipalityName'),
                                    'possession_sheet_type_id': sheet.get('possessionSheetTypeId'),
                                    'possessors': []
                                }
                                
                                # Extract possessors
                                if 'possessors' in sheet and sheet['possessors']:
                                    # Handle both direct array and nested 'possessors' structure
                                    possessors = sheet['possessors']
                                    if isinstance(possessors, dict) and 'possessors' in possessors:
                                        possessors = possessors['possessors']
                                    
                                    if isinstance(possessors, list):
                                        for possessor in possessors:
                                            sheet_data['possessors'].append({
                                                'name': possessor.get('name'),
                                                'ownership': possessor.get('ownership'),
                                                'address': possessor.get('address')
                                            })
                                
                                data['possession_sheets'].append(sheet_data)
                    
                    # Extract parcel links
                    if 'parcelLinks' in content and content['parcelLinks']:
                        # Handle both direct array and nested 'parcelLinks' structure
                        parcel_links = content['parcelLinks']
                        if isinstance(parcel_links, dict) and 'parcelLinks' in parcel_links:
                            parcel_links = parcel_links['parcelLinks']
                        
                        if isinstance(parcel_links, list):
                            for link in parcel_links:
                                link_data = {
                                    'parcel_id': link.get('parcelId'),
                                    'parcel_number': link.get('parcelNumber'),
                                    'address': link.get('address'),
                                    'area': link.get('area')
                                }
                                
                                # Extract LR unit if available
                                if 'lrUnit' in link and link['lrUnit']:
                                    lr_unit = link['lrUnit']
                                    link_data['lr_unit'] = {
                                        'lr_unit_id': lr_unit.get('lrUnitId'),
                                        'lr_unit_number': lr_unit.get('lrUnitNumber'),
                                        'main_book_id': lr_unit.get('mainBookId'),
                                        'main_book_name': lr_unit.get('mainBookName'),
                                        'cadastre_municipality_id': lr_unit.get('cadastreMunicipalityId'),
                                        'status': lr_unit.get('status'),
                                        'verificated': lr_unit.get('verificated'),
                                        'condominiums': lr_unit.get('condominiums')
                                    }
                                
                                data['parcel_links'].append(link_data)
                    
                    # Extract LR units from parcel links
                    if 'lrUnitsFromParcelLinks' in content and content['lrUnitsFromParcelLinks']:
                        # Handle both direct array and nested 'lrUnitsFromParcelLinks' structure
                        lr_units = content['lrUnitsFromParcelLinks']
                        if isinstance(lr_units, dict) and 'lrUnitsFromParcelLinks' in lr_units:
                            lr_units = lr_units['lrUnitsFromParcelLinks']
                        
                        if isinstance(lr_units, list):
                            for lr_unit in lr_units:
                                data['lr_units_from_parcel_links'].append({
                                    'lr_unit_id': lr_unit.get('lrUnitId'),
                                    'lr_unit_number': lr_unit.get('lrUnitNumber'),
                                    'main_book_id': lr_unit.get('mainBookId'),
                                    'main_book_name': lr_unit.get('mainBookName'),
                                    'cadastre_municipality_id': lr_unit.get('cadastreMunicipalityId'),
                                    'institution_id': lr_unit.get('institutionId'),
                                    'institution_name': lr_unit.get('institutionName'),
                                    'status': lr_unit.get('status'),
                                    'status_name': lr_unit.get('statusName'),
                                    'verificated': lr_unit.get('verificated'),
                                    'condominiums': lr_unit.get('condominiums'),
                                    'lr_unit_type_id': lr_unit.get('lrUnitTypeId'),
                                    'lr_unit_type_name': lr_unit.get('lrUnitTypeName')
                                })
                    
                    return data
            except Exception:
                self.failed_requests += 1
                return None

    async def process_batch(self, session: aiohttp.ClientSession, start_id: int) -> List[Dict]:
        # Fetch basic parcel info
        tasks = [self.get_parcel_info(session, start_id + i) for i in range(self.batch_size)]
        results = await asyncio.gather(*tasks)
        valid_results = [r for r in results if r is not None]
        
        # For each parcel, fetch and save detailed land registry data if available
        for parcel_data in valid_results:
            parcel_id = parcel_data.get('parcel_id')
            
            # Check for LR units in parcel links
            for link in parcel_data.get('parcel_links', []):
                if 'lr_unit' in link:
                    lr_unit = link['lr_unit']
                    lr_unit_number = lr_unit.get('lr_unit_number')
                    main_book_id = lr_unit.get('main_book_id')
                    
                    if lr_unit_number and main_book_id:
                        # Fetch detailed land registry data
                        lr_unit_details = await self.get_lr_unit_details(session, lr_unit_number, main_book_id)
                        if lr_unit_details:
                            # Save detailed land registry data
                            self.db.insert_lr_unit_details(lr_unit_details, parcel_id)
            
            # Check for LR units from parcel links
            for lr_unit in parcel_data.get('lr_units_from_parcel_links', []):
                lr_unit_number = lr_unit.get('lr_unit_number')
                main_book_id = lr_unit.get('main_book_id')
                
                if lr_unit_number and main_book_id:
                    # Fetch detailed land registry data
                    lr_unit_details = await self.get_lr_unit_details(session, lr_unit_number, main_book_id)
                    if lr_unit_details:
                        # Save detailed land registry data
                        self.db.insert_lr_unit_details(lr_unit_details, parcel_id)
        
        return valid_results

    async def scrape_range(self, start_id: int, end_id: int):
        timeout = aiohttp.ClientTimeout(total=300)
        connector = aiohttp.TCPConnector(limit=None, limit_per_host=0, force_close=False, ssl=False)
        
        last_id = self.db.get_last_id()
        current_id = last_id + 1 if last_id else start_id
        print(f"Starting from ID: {current_id:,}")

        batch_durations = []
        
        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout, connector=connector) as session:
            while current_id < end_id:
                batch_start_time = time.time()
                remaining_ids = end_id - current_id
                remaining_batches = remaining_ids / self.batch_size
                
                if batch_durations:
                    avg_duration = sum(batch_durations) / len(batch_durations)
                    eta_seconds = avg_duration * remaining_batches
                    eta = str(timedelta(seconds=int(eta_seconds)))
                    stats = self.db.get_stats()
                    print(f"Progress: {current_id:,}/{end_id:,} ({((current_id - start_id) / (end_id - start_id)) * 100:.2f}%) | Records: {stats['count']:,} | ETA: {eta}")
                
                try:
                    batch_results = await self.process_batch(session, current_id)
                    if batch_results:
                        self.db.insert_many(batch_results)
                    
                    batch_duration = time.time() - batch_start_time
                    batch_durations.append(batch_duration)
                    if len(batch_durations) > 50:
                        batch_durations.pop(0)
                    
                    if self.successful_requests % self.batch_size == 0:
                        success_rate = (self.successful_requests / (self.successful_requests + self.failed_requests)) * 100
                        print(f"Success rate: {success_rate:.2f}% | Successful: {self.successful_requests:,} | Failed: {self.failed_requests:,}")
                    
                except Exception as e:
                    print(f"Error in batch {current_id}: {str(e)}")
                    await asyncio.sleep(60)
                    continue
                
                current_id += self.batch_size

    def close(self):
        self.db.close()

async def main():
    scraper = FastCadastralScraper()
    start_id = 18595560
    end_id = 40150506
    
    try:
        await scraper.scrape_range(start_id, end_id)
    except Exception as e:
        print(f"Fatal error: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    asyncio.run(main())
