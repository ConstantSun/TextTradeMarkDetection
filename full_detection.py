import re
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
import boto3
import time
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
from nltk.tokenize import word_tokenize




def normalize_text(string="       Python 3.0, released of  2008, was a major revision of a language that is not completely backward compatible and much Python 2 code does not run unmodified on Python 3. With Python 2's end-of-life, only Python 3.6.x[30] and later are supported, with older versions still supporting e.g. Windows 7 (and old installers not restricted to 64-bit Windows).", num_words = 14) :
    
    # convert to upper case
    upper_string = string.upper()
     
    # remove numbers
    # no_number_string = re.sub(r'\d+','',upper_string)
     
    # remove all punctuation except words and space
    # no_punc_string = re.sub(r'[^\w\s]','', no_number_string)
    
     
    # remove white spaces
    no_wspace_string = " ".join(upper_string.split()[:num_words])
    print(no_wspace_string)
    return no_wspace_string

def get_ngrams(text, n ):
    n_grams = ngrams(word_tokenize(text), n)
    return [ ' '.join(grams) for grams in n_grams]
  

def get_markchar_sn_from_query(query) -> None:
    """Generic function to run athena query and ensures it is successfully completed

    Parameters
    ----------
    query : str
        formatted string containing athena sql query
    s3_output : str
        query output path
        
        
    Return:
    ----------
    char_SN_list : a list of pair[mark_id_char, serial_no]
    
    """
    athena_client = boto3.client("athena")
    
    start_time = time.time()
    
    start_response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'gluejob'
        },
        ResultConfiguration={
            "OutputLocation": 's3://hang-test-query-v1/athena/',
        },
        WorkGroup='primary'
    )
    query_id = start_response["QueryExecutionId"]
    
    char_SN_list = []

    while True:
        finish_state = athena_client.get_query_execution(QueryExecutionId=query_id)[
            "QueryExecution"
        ]["Status"]["State"]
        if finish_state == "RUNNING" or finish_state == "QUEUED":
            # time.sleep(0.2)
            continue
        else:
            response = athena_client.get_query_results(
                QueryExecutionId=query_id,
                # MaxResults=10
            )
            # print("\n#####################\nQUERY Result:\n", response)
            returned_items = response['ResultSet']['Rows']
            num_items = len(returned_items) - 1
            print("number of returned items: ", num_items)
            
            if num_items > 0 :
                for item in returned_items[1:] :
                    mark_id_char =  item["Data"][0]["VarCharValue"]
                    serial_no    =  item["Data"][2]["VarCharValue"]
                    print("****  mark id char: ", mark_id_char)
                    print("****  serial_no   : ", serial_no)
                    char_SN_list.append([mark_id_char,int(serial_no)])
            
            print("--- Execution time : %s  (seconds) ---" % (time.time() - start_time))    

            break

    assert finish_state == "SUCCEEDED", f"query state is {finish_state}"
    print(f"Query {query_id} complete")
    print(char_SN_list)
    return char_SN_list

      

def get_monogram(input_text):
    # ngram = 1 , remove stopword:
    # text_tokens = normalize_text(input_text)

    
    text_tokens = word_tokenize(input_text.lower())
    tokens_without_sw = [word for word in text_tokens if not word in stopwords.words()]
    print(tokens_without_sw)
    single_words_to_check = get_ngrams(" ".join(tokens_without_sw) ,1) 
    single_words_to_check = [word.upper() for word in single_words_to_check]
    return single_words_to_check

  
def input_preprocess(input_text, max_ngram = 5):
    """ 
    Params:
        input_text: string, max_ngram: int
    
    Output:
        words_to_check: set() of words that needs to check in DB (e.g: Athena)
    
    """
    processed_text = normalize_text(input_text)
    print("PROCESSED TEXT: ")
    print(processed_text)
    words_to_check = []
    
    for i in range(2,max_ngram+1):
        words_to_check = get_ngrams(processed_text,i) + words_to_check
        
    words_to_check = words_to_check + get_monogram(input_text)    
    words_to_check = set(words_to_check)    
    print(words_to_check)
    
    return words_to_check    
  
def is_violated(input_text):
    words_to_check = input_preprocess(input_text)
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
    markchar_serialno_list = get_markchar_sn_from_query(query_string)
    violated, mark_drawing_type_pair = check_mark_type(markchar_serialno_list)

    return violated
    
    
def check_mark_type(items:list):
    """ 
    Param:  items - a list of pair[char_mark, serial_num)]
    Return: 
        violation             : boolean, 
        mark_drawing_type_pair: a list of pair(char_mark, mark_type_code)
    """
    
    violation = False
    mark_drawing_type_set = set()  # set of mark_type_code
    mark_drawing_type_pair = [] # list of a pair(char_mark, mark_type_code)
    for item in items:
        serial_no = item[1]
        # -------
        # Todo
        # Call another Lambda to get the Mark Drawing Type from serial_no
        result = ""
        
        if result not in mark_drawing_type_set:
            mark_drawing_type_set.add(result)
            mark_drawing_type_pair.append([item[0], result])
        
        if result == "4 - STANDARD CHARACTER MARK":
            violation = True  # the char_mark has type 4 - standard character mask, so you can not copy this text.
            break
        else:
            continue
    
    return violation, mark_drawing_type_pair
    
    
# print(is_violated("testing"))
    
# text = "Nick likes TO play football, however he Is not Too fond   of tennis."
# text_tokens = word_tokenize(text.lower())

# tokens_without_sw = [word for word in text_tokens if not word in stopwords.words()]

# print(len(stopwords.words()))
# print(normalize_text("he's a good boy, that's the news"))
# print(get_monogram("Devil May Cry Vergil I Need More Power Gaming Gifts Types of Shirts"))

text = "Devil May Cry Vergil I Need More Power Gaming Gifts Types of Shirts"
text2 = "Funny Don't Bu.lly Me ! I'll Cu.m Tee T-Shirt Gift - Classic Unisex Hoodie" # failed
text3 = "Mason Look To The East Personalized Lodge Name AOP Casual Bomber S-5XL"
text4 = "Shirt To Match Jordan 3 Retro Dark Iris 3s Shirt Matching Hoodie All Over Print Shirt 3D Clothing Gifts for Sneakerhead"
text5 = "Freddy Black Red Krueger"
is_violated(text5)
