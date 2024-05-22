import random
import numpy as np
import pandas as pd
from multiprocessing import Pool
from datetime import datetime, timedelta
import os
import time

RECORDS = 1000
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
    'FAST': (1, 3),
    'MEDIUM': (4, 8),
    'SLOW': (9, 18),
    'VERY_SLOW': (19, 30),
}

num_servers = {
    'UI': 20,
    'PURCHASE_BOOK': 10,
    'DISCOUNT': 4,
    'GIFT_WRAP': 1,
    'BOOK_AVAILABILITY': 8,
    'EXTERNAL_INVENT': 2,
    'SECOND_HAND': 2,
    'BUNDLE_OFFER': 2,
    'USER_CREDENTIALS': 8,
    'USER_HISTORY': 5,
    'CARD_CHECK': 10,
    'VISA': 5,
    'A_EXPRESS': 5,
    'MASTERCARD': 5,
    'LIMIT_CHECK_VISA': 20,
    'FRAUD_CHECK_VISA': 5,
    'LIMIT_CHECK_A_EXPRESS': 20,
    'FRAUD_CHECK_A_EXPRESS': 5,
    'LIMIT_CHECK_MC': 20,
    'FRAUD_CHECK_MC': 5,
    'CURRENCY_CONVERSION': 3,
    'SHIPPING_OPTIONS': 2,
    'INSURANCE': 1,
    'EXPRESS': 1,
    'INVENTORY_UPDATE': 20,
    'SUPPLIER_NOTIF': 3,
    'ADJUSTMENTS': 2,
    'REVIEW': 4,
    'REVIEW_VERIFIC': 2,
    'REVIEW_ANALYSIS': 4,
    'AUTO_RESPONSE': 6,
    'AD': 8,
    'CUSTOMER_SUPPORT': 10,
    'HISTORY_ACCESS': 2,
    'PREMIUM_CUSTOMER': 1,
    'AI_CHATBOT': 8
}

# Function to simulate processing delay
def processing_time(delay_range):
    return random.randint(*delay_range)


def server_process(state, server_name, delay_type, num_servers=10):
    prev_time = state["path_times"][-1]

    if num_servers != 1:
        delay = processing_time(delays[delay_type]) + random.randint(1, 2) * (num_servers//4)   # Add a minor server-specific delay
        new_time = prev_time + delay
        server_number = random.randint(1, num_servers)  
        new_server = f'{server_name}_{server_number}'
    else:
        delay = processing_time(delays[delay_type])
        new_time = prev_time + delay
        new_server = f'{server_name}_1'
        
    try:
        prev_server = state['path'][-1].split(", ")[0][1:]
    except:
        prev_server = 'user'
        
    # Correctly formulating log entries
    request_log = f"<{prev_server}, {new_server}, {prev_time}, Request, {state['id']}>"
    response_log = f"<{new_server}, {prev_server}, {new_time}, Response, {state['id']}>"

    state['path'].append(request_log)
    state['path'].append(response_log)
    
    state['path_times'].append(new_time + 1)    # +1 because a server can send the new request a 'second' after it replies
    # state['total_delay'] += delay
    
    return state

def simulate_process(data_id, unique_id):
    # State tracking for the process
    state = {
        'id': f'{unique_id:010d}',
        'path': [],
        'path_times': [random.randint(0, 1000)],
        'total_delay': 0,
        'discount_gift_wrap_loop': False
    }
    
    ###########################################################################################################################
    
    # Define server functions
    def ui_server(state):
        state = server_process(state, 'ui_server', 'FAST', num_servers['UI'])
        purchase_book_server(state)

    ###########################################################################################################################
    
    def purchase_book_server(state):
        discount_applied = False
        state = server_process(state, 'purchase_book_server', 'MEDIUM', num_servers['PURCHASE_BOOK'])

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
        state = server_process(state, 'discount_server', 'MEDIUM',num_servers['DISCOUNT'])

        if not state['discount_gift_wrap_loop']:
          if random.random() < probabilities['DISCOUNT_GIFT_WRAP']:
              state['discount_gift_wrap_loop'] = True
              gift_wrap_server(state)


    def gift_wrap_server(state):
        state = server_process(state, 'discount_server', 'FAST', num_servers['GIFT_WRAP'])

        if not state['discount_gift_wrap_loop']:
            if random.random() < probabilities['DISCOUNT_GIFT_WRAP']:
                state['discount_gift_wrap_loop'] = True
                discount_server(state)

    ###########################################################################################################################
    
    def book_availability_server(state):
        state = server_process(state, 'book_availability_server', 'SLOW', num_servers['BOOK_AVAILABILITY'])

        # Optional calls
        if random.random() < probabilities['EXTERNAL_INVENT']:
            external_inventory(state)

        if random.random() < probabilities['BUNDLE_OFFER']:
            bundle_offer(state)

        user_credentials(state)  # Mandatory call


    def external_inventory(state):
        state = server_process(state, 'external_inventory_server', 'MEDIUM', num_servers['EXTERNAL_INVENT'])

        # Optional calls
        if random.random() < probabilities['SECOND_HAND']:
            second_hand_market(state)


    def second_hand_market(state):
        state = server_process(state, 'second_hand_market_server', 'VERY_SLOW', num_servers['SECOND_HAND'])


    def bundle_offer(state):
        state = server_process(state, 'bundle_offer_server', 'MEDIUM', num_servers['BUNDLE_OFFER'])

    ###########################################################################################################################
    
    def user_credentials(state):
        state = server_process(state, 'user_credentials_server', 'FAST', num_servers['USER_CREDENTIALS'])

        # Optional calls
        if random.random() < probabilities['USER_HISTORY']:
            user_history(state)

        card_check(state)  # Mandatory call


    def user_history(state):
        state = server_process(state, 'user_history_server', 'MEDIUM', num_servers['USER_HISTORY'])

    ###########################################################################################################################
    
    def card_check(state):
        state = server_process(state, 'card_check_server', 'MEDIUM', num_servers['CARD_CHECK'])

        if random.random() < probabilities['VISA']:
            visa(state)
        elif random.random() < probabilities['VISA'] + probabilities['A_EXPRESS']:
            american_express(state)
        else:
            mastercard(state)


    def visa(state):
        state = server_process(state, 'visa_server', 'SLOW', num_servers['VISA'])

        # Optional calls
        if random.random() < probabilities['LIMIT_CHECK_VISA']:
            limit_check_visa(state)

        if random.random() < probabilities['FRAUD_CHECK_VISA']:
            fraud_check_visa(state)

        currency_conversion(state)  # Mandatory call


    def limit_check_visa(state):
        state = server_process(state, 'limit_check_visa_server', 'VERY_SLOW', num_servers['LIMIT_CHECK_VISA'])


    def fraud_check_visa(state):
        state = server_process(state, 'fraud_check_visa_server', 'SLOW', num_servers['FRAUD_CHECK_VISA'])


    def american_express(state):
        state = server_process(state, 'american_express_server', 'MEDIUM', num_servers['A_EXPRESS'])

        # Optional calls
        if random.random() < probabilities['LIMIT_CHECK_A_EXPRESS']:
            limit_check_american_express(state)

        if random.random() < probabilities['FRAUD_CHECK_A_EXPRESS']:
            fraud_check_american_express(state)

        currency_conversion(state)  # Mandatory call


    def limit_check_american_express(state):
        state = server_process(state, 'limit_check_american_express_server', 'VERY_SLOW', num_servers['LIMIT_CHECK_A_EXPRESS'])


    def fraud_check_american_express(state):
        state = server_process(state, 'fraud_check_american_express_server', 'MEDIUM', num_servers['LIMIT_CHECK_A_EXPRESS'])


    def mastercard(state):
        state = server_process(state, 'mastercard_server', 'SLOW', num_servers['MASTERCARD'])

        # Optional calls
        if random.random() < probabilities['LIMIT_CHECK_MC']:
            limit_check_mastercard(state)

        if random.random() < probabilities['FRAUD_CHECK_MC']:
            fraud_check_mastercard(state)

        currency_conversion(state)  # Mandatory call


    def limit_check_mastercard(state):
        state = server_process(state, 'limit_check_mastercard_server', 'VERY_SLOW', num_servers['LIMIT_CHECK_MC'])


    def fraud_check_mastercard(state):
        state = server_process(state, 'fraud_check_mastercard_server', 'MEDIUM', num_servers['FRAUD_CHECK_MC'])

    ###########################################################################################################################
    
    def currency_conversion(state):
        state = server_process(state, 'currency_conversion_server', 'FAST', num_servers['CURRENCY_CONVERSION'])

        shipping_options(state)  # Mandatory call

    ###########################################################################################################################
    
    def shipping_options(state):
        state = server_process(state, 'shipping_options_server', 'MEDIUM', num_servers['SHIPPING_OPTIONS'])

        # Optional calls
        if random.random() < probabilities['INSURANCE']:
            shipping_insurance(state)

        if random.random() < probabilities['EXPRESS']:
            express_delivery(state)

        inventory_update(state)  # Mandatory call


    def shipping_insurance(state):
        state = server_process(state, 'shipping_insurance_server', 'MEDIUM', num_servers['INSURANCE'])


    def express_delivery(state):
        state = server_process(state, 'express_delivery_server', 'MEDIUM', num_servers['EXPRESS'])

    ###########################################################################################################################
    
    def inventory_update(state):
        state = server_process(state, 'inventory_update_server', 'VERY_SLOW', num_servers['INVENTORY_UPDATE'])

        # Optional calls
        if random.random() < probabilities['SUPPLIER_NOTIF']:
            supplier_notification(state)

        if random.random() < probabilities['ADJUSTMENTS']:
            seasonal_adjustments(state)

        review(state)  # Mandatory call


    def supplier_notification(state):
        state = server_process(state, 'supplier_notification_server', 'SLOW', num_servers['SUPPLIER_NOTIF'])


    def seasonal_adjustments(state):
        state = server_process(state, 'seasonal_adjustments_server', 'VERY_SLOW', num_servers['ADJUSTMENTS'])

    ###########################################################################################################################
    
    def review(state):
        state = server_process(state, 'review_server', 'VERY_SLOW', num_servers['REVIEW'])

        # Optional calls
        if random.random() < probabilities['REVIEW_VERIFIC']:
            review_verification(state)

        if random.random() < probabilities['REVIEW_ANALYSIS']:
            review_analysis(state)

        if random.random() < probabilities['AUTO_RESPONSE']:
            automatic_response(state)

        ad(state)  # Mandatory call


    def review_verification(state):
        state = server_process(state, 'review_verification_server', 'SLOW', num_servers['REVIEW_VERIFIC'])


    def review_analysis(state):
        state = server_process(state, 'review_analysis_server', 'SLOW', num_servers['REVIEW_ANALYSIS'])


    def automatic_response(state):
        state = server_process(state, 'automatic_response_server', 'MEDIUM', num_servers['AUTO_RESPONSE'])

    ###########################################################################################################################
    
    def ad(state):
        state = server_process(state, 'ad_server', 'SLOW', num_servers['AD'])

        customer_support(state)  # Mandatory call

    ###########################################################################################################################
    
    def customer_support(state):
        state = server_process(state, 'customer_support_server', 'SLOW', num_servers['CUSTOMER_SUPPORT'])

        # Optional calls
        if random.random() < probabilities['HISTORY_ACCESS']:
            customer_history_access(state)

        if random.random() < probabilities['PREMIUM_CUSTOMER']:
            premium_customer_check(state)


    def customer_history_access(state):
        state = server_process(state, 'customer_history_access_server', 'SLOW', num_servers['HISTORY_ACCESS'])


    def premium_customer_check(state):
        state = server_process(state, 'premium_customer_check_server', 'SLOW', num_servers['PREMIUM_CUSTOMER'])

        # Optional calls
        if random.random() < probabilities['AI_CHATBOT']:
            ai_chatbot(state)


    def ai_chatbot(state):
        state = server_process(state, 'ai_chatbot_server', 'MEDIUM', num_servers['AI_CHATBOT'])
        
    ###########################################################################################################################


    # Start the process
    ui_server(state)

    # formatted_timestamps = [dt.strftime("%H:%M:%S.%f")[:-4] for dt in state['path_times']]

    return state['path']


# Use a pool of workers to process data in parallel
if __name__ == '__main__':
    # Set the current directory to the directory of the script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    print("Current Working Directory:", os.getcwd())

    start = time.time()    

    for i in range(ITERATIONS):
        with Pool(NUM_PROCESSES) as pool:
            tasks = [(data_id, i * RECORDS + j + 1) for j, data_id in enumerate(range(i * RECORDS, (i + 1) * RECORDS))]
            
            results = pool.starmap(simulate_process, tasks)
        
        
        name = f'datasets/dataset_{i+1}.csv'
        # Keep each call as a separate entry in the dataframe
        flat_list = [item for sublist in results for item in sublist]

        # Convert results to DataFrame
        data = pd.DataFrame(flat_list, columns=['Logs'])
        
        data['time'] = data['Logs'].apply(lambda x: int(x.split(',')[2].strip()))
        
        data_sorted = data.sort_values(by='time', ascending=True)
        
        data_sorted.drop(columns=["time"], inplace=True)


        # Save the file
        data_sorted.to_csv(name, index=False)

        print(f"Iteration {i + 1}: Data appended to {name}")
    
    end = time.time()
    print("Total time:", end - start)
    print("Total users:", ITERATIONS * RECORDS)
