import argparse
import lambda_handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="yyyy-mm-dd")
    args = parser.parse_args()

    event = {
        "date": args.date,
    }

    lambda_handler.lambda_handler(event, None)


if __name__ == "__main__":
    main()
