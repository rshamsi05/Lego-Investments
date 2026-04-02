import pytest
from google.cloud import bigquery
from storage.schema import (
    SCHEMA_STG_SETS,
    SCHEMA_STG_PRICES,
    SCHEMA_STG_MINIFIGURES,
    SCHEMA_STG_PARTS,
    SCHEMA_STG_SET_MINIFIGURES,
    SCHEMA_STG_SET_PARTS,
    SCHEMA_SETS,
    SCHEMA_PRICE_HISTORY,
    SCHEMA_MINIFIGURES,
    SCHEMA_PARTS,
    SCHEMA_SET_PERFORMANCE,
)

class TestSchemaDefinitions:
    '''Test that all schema definitions are valid'''
    
    def test_all_schemas_are_lists(self):
        '''All schemas should be lists of SchemaField objects'''
        schemas = [
            SCHEMA_STG_SETS,
            SCHEMA_STG_PRICES,
            SCHEMA_STG_MINIFIGURES,
            SCHEMA_STG_PARTS,
            SCHEMA_STG_SET_MINIFIGURES,
            SCHEMA_STG_SET_PARTS,
            SCHEMA_SETS,
            SCHEMA_PRICE_HISTORY,
            SCHEMA_MINIFIGURES,
            SCHEMA_PARTS,
            SCHEMA_SET_PERFORMANCE,
        ]
        
        for schema in schemas:
            assert isinstance(schema, list), f"Schema should be a list: {schema}"
            for field in schema:
                assert isinstance(field, bigquery.SchemaField), \
                    f"All fields should be SchemaField objects: {field}"
    
    def test_stg_sets_has_required_fields(self):
        '''Staging sets table should have required fields'''
        field_names = [field.name for field in SCHEMA_STG_SETS]
        
        required_fields = ['set_id', 'name', 'year', 'is_retired', 'ingested_at']
        for field in required_fields:
            assert field in field_names, f"Missing required field: {field}"
    
    def test_warehouse_sets_has_investment_metrics(self):
        '''Warehouse sets table should have investment metric fields'''
        field_names = [field.name for field in SCHEMA_SETS]
        
        investment_fields = [
            'price_appreciation_percentage',
            'annualized_return_percentage',
            'days_since_retirement',
            'era_classification',
        ]
        for field in investment_fields:
            assert field in field_names, f"Missing investment field: {field}"
    
    def test_all_fields_have_descriptions(self):
        '''All fields should have descriptions for documentation'''
        schemas = [
            SCHEMA_SETS,
            SCHEMA_PRICE_HISTORY,
            SCHEMA_MINIFIGURES,
            SCHEMA_PARTS,
            SCHEMA_SET_PERFORMANCE,
        ]
        
        for schema in schemas:
            for field in schema:
                assert field.description is not None and field.description.strip(), \
                    f"Field '{field.name}' in {schema} is missing description"
    
    def test_set_performance_has_rankings(self):
        '''Set performance table should have ranking fields'''
        field_names = [field.name for field in SCHEMA_SET_PERFORMANCE]

        ranking_fields = ['theme_rank_roi', 'theme_rank_by_appreciation']
        for field in ranking_fields:
            assert field in field_names, f"Missing ranking field: {field}"
