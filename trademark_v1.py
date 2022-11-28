import time
import json
import os
from util import preprocess_input, get_found_markchar_sn_list_from_query, check_mark_type_n_usage
import boto3

s3_client = boto3.client('s3', region_name='ap-southeast-1')

def lambda_handler(event, context):
    """
    Step 1 : Preprocess Input: Return a set of words (ngrams)
    Step 2 : Given a set of words, run queryies, return an array of pair[char_mark, serial_no]
    Step 3 : Given an array of pair[char_mark, serial_no], crawling via USPTO api and return violated check mark.
    """
    start_time = time.time()
    img_url = os.environ['image_url']
    input_text = os.environ['text']
    
    words_to_check = preprocess_input(input_text)
    print("length: ", len(words_to_check), "\n----\n")
    query_list_words = "('" +  "', '".join(words_to_check) + "\"', '\"".join(words_to_check) +"\"')"
    print(query_list_words)
    
    query_string = """
                    SELECT mark_id_char,abandon_dt, serial_no
                    FROM """ + os.environ['glue_table_name'] + """
                    WHERE  mark_id_char IN """ + str(query_list_words) +  """
                    AND 
                    (TRIM(abandon_dt) IS NULL OR LTRIM(RTRIM(abandon_dt)) = '' )
                    ;    
    """
    print("\n\nQuery string: ", query_string)
    markchar_serialno_list = get_found_markchar_sn_list_from_query(query_string)
    sn_error_list, violated, violated_mark_drawing_type_pair = check_mark_type_n_usage(markchar_serialno_list)
    print("------------\n CONCLUSION: ")
    print("VIOLATED: ", violated)
    print("Mark drawing type pair: ", violated_mark_drawing_type_pair)
    
    running_time = time.time() - start_time
    print("--- TOTAL: %s seconds ---" % (running_time))

    violated_details = {
        "input_text": input_text,
        "serial_no_err_list": sn_error_list,
        "violated": int(violated == True),
        "processed_time_in_second":running_time,
        "violated_mark_drawing_type_pair": violated_mark_drawing_type_pair
    }

    response = {
        "image_url": img_url,
        "text": input_text,
        "violation": int(violated == True),
        "description": violated_details
    }

            
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
    
