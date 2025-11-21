from src.analysis import generate_status_report

def main():
    print("Running analysis...")
    report = generate_status_report()
    print(report)

if __name__ == "__main__":
    main()
