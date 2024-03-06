# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys

from solution.producer_sol import mqProducer  # pylint: disable=import-error


def main(ticker: str, price: float, sector: str) -> None:

    # Step 2: Create Routing Key from the ticker and sector variable
    routingKey = f"stock.{sector}.{ticker}"

    producer = mqProducer(routing_key=routingKey,exchange_name="Tech Lab Topic Exchange")

    # Step 3: Create a message variable from the ticker and price
    message = f"{ticker} price is now ${price}"
    
    # Publish the order
    producer.publishOrder(message=message)

if __name__ == "__main__":

    # Step 1: Parse command-line arguments
    parser = argparse.ArgumentParser(description="Publish stock price updates to RabbitMQ.")
    parser.add_argument('ticker', type=str, help='Stock ticker symbol')
    parser.add_argument('price', type=float, help='Current stock price')
    parser.add_argument('sector', type=str, help='Sector of the stock')

    args = parser.parse_args()

    # Extract variables from parsed arguments
    ticker = args.ticker
    price = args.price
    sector = args.sector

    # Implement Logic to read the ticker, price and sector string from the command line and save them - Step 1
    #
    #                       WRITE CODE HERE!!!
    #

    sys.exit(main(ticker,price,sector))
