'''
Test script to verify Bricklink price scraping works
'''

print("Script Started for bricklink")
from ingestion.bricklink import BrickLinkIngestion

def main():
    print("Starting BrickLink ingestion test...")

    ingestion = BrickLinkIngestion()

    # Testing with a few popular sets to verify price history scraping works
    test_sets = [
        '75252-1', # Star Wars UCS Millennium Falcon
        '21323-1', # NASA Apollo Saturn V
        '10276-1'  # Colosseum
    ]

    # Running ingestion process for test sets
    ingestion.ingest(test_sets)

    print("BrickLink ingestion test completed successfully")


if __name__ == "__main__":
    main()
