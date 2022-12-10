import argparse
import get_units_lambda


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="yyyy-mm-dd")
    args = parser.parse_args()

    event = {
        "date": args.date,
    }
    get_units_lambda.lambda_handler(event, None)


if __name__ == "__main__":
    main()
