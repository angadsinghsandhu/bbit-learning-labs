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

from solution.consumer_sol import mqConsumer  # pylint: disable=import-error

def main(sector: str, queueName: str) -> None:
    
    # Implement Logic to Create Binding Key from the ticker and sector variable -  Step 2
    #
    #                       WRITE CODE HERE!!!
    #
    # Step 2: Create Binding Key from the sector variable
    # Assuming we want to consume messages for all tickers within the given sector
    bindingKey = f"stock.{sector}.*"
    
    consumer = mqConsumer(binding_key=bindingKey,exchange_name="Tech Lab Topic Exchange",queue_name=queueName)    
    consumer.startConsuming()
    


if __name__ == "__main__":
    # Step 1: Parse command-line arguments
    parser = argparse.ArgumentParser(description="Consume stock price updates from RabbitMQ.")
    parser.add_argument('sector', type=str, help='Sector of the stock to subscribe to')
    parser.add_argument('queue', type=str, help='Name of the queue to consume from')

    args = parser.parse_args()

    # Extract variables from parsed arguments
    sector = args.sector
    queue = args.queue

    sys.exit(main(sector, queue))
