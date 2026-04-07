'''
Test script to verify Rebrickable ingestion works
'''

from ingestion.rebrickable import RebrickableIngestion

def main():
    ingestion = RebrickableIngestion()

    # Running ingestion process
    ingestion.ingest()

    print("Rebrickable ingestion completed successfully")

if __name__ == "__main__":
    main()
