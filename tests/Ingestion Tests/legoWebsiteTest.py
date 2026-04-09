'''
Test Script to verify Lego.com scraping works
'''
from ingestion.lego_site import LegoSiteIngestion

def main():
    print("Starting Lego.com scraping test...")

    ingestion = LegoSiteIngestion()

    # Defining star wars sets to test with
    test_sets = [
        "75192", # UCS Millennium Falcon
        "75367", # Venator-Class Republic Attatck Cruiser
        "75331", # Razor Crest Interceptor
    ]

    # Running ingestion process
    ingestion.ingest(test_sets)

    print("Lego.com scraping test completed successfully")


if __name__ == "__main__":
    main()
