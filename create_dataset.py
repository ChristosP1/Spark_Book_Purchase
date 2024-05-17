import random
import numpy as np
import pandas as pd
from multiprocessing import Pool
from datetime import datetime, timedelta
import os
import time

RECORDS = 500000
NUM_PROCESSES = 16
ITERATIONS = 10


probabilities = {
    'DISCOUNT': 0.3,
    'GIFT_WRAP': 0.2,
    'DISCOUNT_GIFT_WRAP': 0.3,
    'EXTERNAL_INVENT': 0.15,
    'SECOND_HAND': 0.6,
    'BUNDLE_OFFER': 0.7,
    'USER_HISTORY': 0.5,
    'VISA': 0.6,
    'A_EXPRESS': 0.3,
    'MASTERCARD': 0.1,
    'LIMIT_CHECK_VISA': 0.8,
    'FRAUD_CHECK_VISA': 0.6,
    'LIMIT_CHECK_A_EXPRESS': 0.8,
    'FRAUD_CHECK_A_EXPRESS': 0.6,
    'LIMIT_CHECK_MC': 0.8,
    'FRAUD_CHECK_MC': 0.6,
    'INSURANCE': 0.15,
    'EXPRESS': 0.5,
    'SUPPLIER_NOTIF': 0.4,
    'ADJUSTMENTS': 0.3,
    'REVIEW_VERIFIC': 0.8,
    'REVIEW_ANALYSIS': 0.6,
    'AUTO_RESPONSE': 0.2,
    'HISTORY_ACCESS': 0.1,
    'PREMIUM_CUSTOMER': 0.1,
    'AI_CHATBOT': 0.15
}

# IN SECONDS
delays = {
    'FAST': (0.01, 0.03),
    'MEDIUM': (0.1, 0.4),
    'SLOW': (0.4, 0.8),
    'VERY_SLOW': (0.8, 1.3),
}

# Function to simulate processing delay
def processing_time(delay_range):
    return random.uniform(*delay_range)


def random_timestamp():
    # Generate a random time component
    hours = random.randint(0, 23)
    minutes = random.randint(0, 59)
    seconds = random.randint(0, 59)
    milliseconds = random.randint(0, 999)

    # Create a datetime object for today with the random time
    now = datetime.now()

    # Return the datetime object
    return datetime(now.year, now.month, now.day, hours, minutes, seconds, milliseconds * 1000)


def simulate_process(i):
    # State tracking for the process
    state = {
        'path': [],
        'path_times': [random_timestamp()],
        'total_delay': 0,
        'discount_gift_wrap_loop': False
    }
    
    ###########################################################################################################################
    
    # Define server functions
    def ui_server(state):
        state['path'].append('ui_server')
        delay = processing_time(delays['FAST'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay
        purchase_book_server(state)

    ###########################################################################################################################
    
    def purchase_book_server(state):
        discount_applied = False

        state['path'].append('purchase_book_server')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['DISCOUNT']:
            discount_server(state)
            discount_applied = True

        if random.random() < probabilities['GIFT_WRAP']:
            if discount_applied:
                state['discount_gift_wrap_loop'] = True
            if not state['discount_gift_wrap_loop']:
                gift_wrap_server(state)

        book_availability_server(state)  # Mandatory call


    def discount_server(state):
        state['path'].append('discount_server')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        if not state['discount_gift_wrap_loop']:
          if random.random() < probabilities['DISCOUNT_GIFT_WRAP']:
              state['discount_gift_wrap_loop'] = True
              gift_wrap_server(state)


    def gift_wrap_server(state):
        state['path'].append('gift_wrap_server')
        delay = processing_time(delays['FAST'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        if not state['discount_gift_wrap_loop']:
            if random.random() < probabilities['DISCOUNT_GIFT_WRAP']:
                state['discount_gift_wrap_loop'] = True
                discount_server(state)

    ###########################################################################################################################
    
    def book_availability_server(state):
        state['path'].append('book_availability_server')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['EXTERNAL_INVENT']:
            external_inventory(state)

        if random.random() < probabilities['BUNDLE_OFFER']:
            bundle_offer(state)

        user_credentials(state)  # Mandatory call


    def external_inventory(state):
        state['path'].append('external_inventory')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['SECOND_HAND']:
            second_hand_market(state)


    def second_hand_market(state):
        state['path'].append('second_hand_market')
        delay = processing_time(delays['VERY_SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def bundle_offer(state):
        state['path'].append('bundle_offer')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

    ###########################################################################################################################
    
    def user_credentials(state):
        state['path'].append('user_credentials')
        delay = processing_time(delays['FAST'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['USER_HISTORY']:
            user_history(state)

        card_check(state)  # Mandatory call


    def user_history(state):
        state['path'].append('user_history')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

    ###########################################################################################################################
    
    def card_check(state):
        state['path'].append('card_check')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += processing_time(delays['MEDIUM'])

        if random.random() < probabilities['VISA']:
            visa(state)
        elif random.random() < probabilities['VISA'] + probabilities['A_EXPRESS']:
            american_express(state)
        else:
            mastercard(state)


    def visa(state):
        state['path'].append('visa')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['LIMIT_CHECK_VISA']:
            limit_check_visa(state)

        if random.random() < probabilities['FRAUD_CHECK_VISA']:
            fraud_check_visa(state)

        currency_conversion(state)  # Mandatory call


    def limit_check_visa(state):
        state['path'].append('limit_check_visa')
        delay = processing_time(delays['VERY_SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def fraud_check_visa(state):
        state['path'].append('fraud_check_visa')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def american_express(state):
        state['path'].append('american_express')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['LIMIT_CHECK_A_EXPRESS']:
            limit_check_american_express(state)

        if random.random() < probabilities['FRAUD_CHECK_A_EXPRESS']:
            fraud_check_american_express(state)

        currency_conversion(state)  # Mandatory call


    def limit_check_american_express(state):
        state['path'].append('limit_check_american_express')
        delay = processing_time(delays['VERY_SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def fraud_check_american_express(state):
        state['path'].append('fraud_check_american_express')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def mastercard(state):
        state['path'].append('mastercard')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['LIMIT_CHECK_MC']:
            limit_check_mastercard(state)

        if random.random() < probabilities['FRAUD_CHECK_MC']:
            fraud_check_mastercard(state)

        currency_conversion(state)  # Mandatory call


    def limit_check_mastercard(state):
        state['path'].append('limit_check_mastercard')
        delay = processing_time(delays['VERY_SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def fraud_check_mastercard(state):
        state['path'].append('fraud_check_mastercard')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

    ###########################################################################################################################
    
    def currency_conversion(state):
        state['path'].append('currency_conversion')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        shipping_options(state)  # Mandatory call

    ###########################################################################################################################
    
    def shipping_options(state):
        state['path'].append('shipping_options')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['INSURANCE']:
            shipping_insurance(state)

        if random.random() < probabilities['EXPRESS']:
            express_delivery(state)

        inventory_update(state)  # Mandatory call


    def shipping_insurance(state):
        state['path'].append('shipping_insurance')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def express_delivery(state):
        state['path'].append('express_delivery')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

    ###########################################################################################################################
    
    def inventory_update(state):
        state['path'].append('inventory_update')
        delay = processing_time(delays['VERY_SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['SUPPLIER_NOTIF']:
            supplier_notification(state)

        if random.random() < probabilities['ADJUSTMENTS']:
            seasonal_adjustments(state)

        review(state)  # Mandatory call


    def supplier_notification(state):
        state['path'].append('supplier_notification')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def seasonal_adjustments(state):
        state['path'].append('seasonal_adjustments')
        delay = processing_time(delays['VERY_SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

    ###########################################################################################################################
    
    def review(state):
        state['path'].append('review')
        delay = processing_time(delays['VERY_SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += processing_time(delays['MEDIUM'])

        # Optional calls
        if random.random() < probabilities['REVIEW_VERIFIC']:
            review_verification(state)

        if random.random() < probabilities['REVIEW_ANALYSIS']:
            review_analysis(state)

        if random.random() < probabilities['AUTO_RESPONSE']:
            automatic_response(state)

        ad(state)  # Mandatory call


    def review_verification(state):
        state['path'].append('review_verification')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def review_analysis(state):
        state['path'].append('review_analysis')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def automatic_response(state):
        state['path'].append('automatic_response')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

    ###########################################################################################################################
    
    def ad(state):
        state['path'].append('ad')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        customer_support(state)  # Mandatory call

    ###########################################################################################################################
    
    def customer_support(state):
        state['path'].append('customer_support')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['HISTORY_ACCESS']:
            customer_history_access(state)

        if random.random() < probabilities['PREMIUM_CUSTOMER']:
            premium_customer_check(state)


    def customer_history_access(state):
        state['path'].append('customer_history_access')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay


    def premium_customer_check(state):
        state['path'].append('premium_customer_check')
        delay = processing_time(delays['SLOW'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay

        # Optional calls
        if random.random() < probabilities['AI_CHATBOT']:
            ai_chatbot(state)


    def ai_chatbot(state):
        state['path'].append('ai_hatbot')
        delay = processing_time(delays['MEDIUM'])
        new_time = state['path_times'][-1] + timedelta(seconds=delay)
        state['path_times'].append(new_time)
        state['total_delay'] += delay
        
    ###########################################################################################################################


    # Start the process
    ui_server(state)

    formatted_timestamps = [dt.strftime("%H:%M:%S.%f")[:-4] for dt in state['path_times']]

    return {'path': state['path'], 'timestamps': formatted_timestamps, 'Total Delay': round(state['total_delay'], 2)}


# Use a pool of workers to process data in parallel
if __name__ == '__main__':
    # Set the current directory to the directory of the script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    print("Current Working Directory:", os.getcwd())

    start = time.time()

    # Define the filename
    name = 'datasets/dataset.csv'
    
    start = time.time()

    for i in range(ITERATIONS):
        with Pool(NUM_PROCESSES) as pool:
            results = pool.map(simulate_process, range(RECORDS))

        # Convert results to DataFrame
        data = pd.DataFrame(results)

        # Check if file exists and append if it does, else write a new file
        if os.path.exists(name):
            data.to_csv(name, mode='a', header=False, index=False)
        else:
            data.to_csv(name, index=False)

        print(f"Iteration {i + 1}: Data appended to {name}")
    
    end = time.time()
    print("Total time:", end - start)
    print("Total users:", ITERATIONS * RECORDS)
