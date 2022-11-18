import re
import boto3
import time
import json
from util import preprocess_input, get_found_markchar_sn_list_from_query, check_mark_type_n_usage
  
text = "Devil May Cry Vergil I Need More Power Gaming Gifts Types of Shirts"
text2 = "Funny Don't Bu.lly Me ! I'll Cu.m Tee T-Shirt Gift - Classic Unisex Hoodie" # failed
text3 = "Mason Look To The East Personalized Lodge Name AOP Casual Bomber S-5XL"
text4 = "Shirt To Match Jordan 3 Retro Dark Iris 3s Shirt Matching Hoodie All Over Print Shirt 3D Clothing Gifts for Sneakerhead"
text5 = "Freddy Black Red Krueger"
# is_violated(text5)

def lambda_handler(event, context):
    """
    Step 1 : Preprocess Input: Return a set of words (ngrams)
    Step 2 : Given a set of words, run queryies, return an array of pair[char_mark, serial_no]
    Step 3 : Given an array of pair[char_mark, serial_no], crawl via puppeteer and return violated check mark.
    """
    start_time = time.time()
    img_url = event['img_url']
    words_to_check = preprocess_input(event['text'])
    print("length: ", len(words_to_check), "\n----\n")
    query_list_words = "('" +  "', '".join(words_to_check) + "\"', '\"".join(words_to_check) +"\"')"
    print(query_list_words)
    
    query_string = """
                    SELECT mark_id_char,abandon_dt, serial_no
                    FROM "us-trademark-v2-updated"."gluejob"
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

    final_result = {
        "input_text": event['text'],
        "serial_no_err_list": sn_error_list,
        "violated": violated,
        "processed_time_in_second":running_time,
        "violated_mark_drawing_type_pair": violated_mark_drawing_type_pair
    }
    # Saving result to json file
    # Serializing json
    json_object = json.dumps(final_result, indent=4)
     
    # Writing to sample.json
    with open(f"sample_{event['number']}.json", "w") as outfile:
        outfile.write(json_object)    
        
    response = {
        "image_url": img_url,
        "text": event['text'],
        "violation": violated,
        "description": violated_details
    }
    return {
        'image_url': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    

text_arr = [
    "Apex Legends Valkyrie 80s Retro Videos Game Wall Art Poster",
    "Valorant Sage So That's What It Feels Like Minimalist Decor Wall Art Poster",
    "Deltarune Ralsei Smoking A Fat Blunt Hope Style Video Game Wall Art Poster",
    "Red Dead Redemption Arthur Morgan We're More Ghosts Than People Gaming Gifts Types of Shirts",
    "Deltarune Jevil Now's Your Chance! Video Game Wall Art Poster",
    "Borderlands Gun Skull Psycho Bandit Video Game Wall Art Poster",
    "Kusum Dirty Dancing 35th Anniversary 1987 2022 Thank You for Memories Signature Characters Wall Art Poster",
    
    "Sanford And Son 50th Anniversary 1972 2022 Thank You For Memories Signature Characters Ceramic Coffee Mug, Tea Cup",
    "Ratchet and Clank Ratchet (7) Video Game Types of Shirts",
    "Valorant Cypher Nothing Stays Hidden from Me. Nothing Minimalist Decor Wall Art Poster",
    "Melanie Valorant Reyna?? Duelist Empress Retro Video Game Wall Art Poster",
    "Albert Wesker Complete Global Saturation Gaming Gifts Types of Shirts",
    "Deltarune Kris and Ralsei and Susie Rune Delta Spark Video Game Types of Shirts",
    "Resident Evil Village Albert Wesker Complete Global Saturation Minimalist Decor Wall Art Poster 18x24 Inch"
    ]
if __name__ == "__main__":
    for i in range(len(text_arr)):
        event = {
            "input_text" : text_arr[i],
            "number": i
        }
        lambda_handler(event,None)
