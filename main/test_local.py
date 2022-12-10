import argparse
import lambda_handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="yyyy-mm-dd")
    parser.add_argument("-a", "--advertising_account_id", help="advertising_account_id")
    parser.add_argument("-p", "--portfolio_id", help="portfolio_id", default=None)
    args = parser.parse_args()

    event = {
        "date": args.date,
        "advertising_account_id": args.advertising_account_id,
        "portfolio_id": args.portfolio_id,
    }
    lambda_handler.lambda_handler(event, None)


if __name__ == "__main__":
    main()
