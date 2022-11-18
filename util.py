import re
import boto3
import time
from boto3 import client as boto3_client
from boto3.dynamodb.conditions import Key
from datetime import datetime
import json

lambda_client = boto3_client('lambda', region_name = "ap-southeast-1")
comprehend_client = boto3_client('comprehend', region_name = "ap-southeast-1")
dynamodb = boto3.resource('dynamodb', region_name = "ap-southeast-1")
table = dynamodb.Table("CrawledItems1")
ssm = boto3.client('ssm', region_name='ap-southeast-1')


STOP_WORDS = { 'able', 'about', 'above', 'accha', 'according', 'accordingly', 'acha', 'achcha', 'across', 'actually', 'after', 'afterwards', 'again', 'against', 'agar', 'ain', 'aint', "ain't", 'aisa', 'aise', 'aisi', 'alag', 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'andar', 'another', 'any', 'anybody', 'anyhow', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'ap', 'apan', 'apart', 'apna', 'apnaa', 'apne', 'apni', 'appear', 'are', 'aren', 'arent', "aren't", 'around', 'arre', 'as', 'aside', 'ask', 'asking', 'at', 
'aur', 'avum', 'aya', 'aye', 'baad', 'baar', 'bad', 'bahut', 'bana', 'banae', 'banai', 'banao', 'banaya', 'banaye', 'banayi', 'banda', 'bande', 'bandi', 'bane', 'bani', 'bas', 'bata', 'batao', 'bc', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'below', 'beside', 'besides', 'best', 'better', 'between', 'beyond', 'bhai', 'bheetar', 'bhi', 'bhitar', 'bht', 'bilkul', 'bohot', 'bol', 'bola', 'bole', 'boli', 'bolo', 'bolta', 'bolte', 'bolti', 'both', 'brief', 'bro', 'btw', 'but', 'by', 'came', 'can', 'cannot', 'cant', "can't", 'cause', 'causes', 'certain', 'certainly', 'chahiye', 'chaiye', 'chal', 'chalega', 'chhaiye', 'clearly', "c'mon", 'com', 'come', 'comes', 'could', 'couldn', 'couldnt', "couldn't", 'd', 'de', 'dede', 'dega', 'degi', 'dekh', 'dekha', 'dekhe', 'dekhi', 'dekho', 'denge', 'dhang', 'di', 'did', 'didn', 'didnt', "didn't", 'dijiye', 'diya', 'diyaa', 'diye', 'diyo', 'do', 'does', 'doesn', 'doesnt', "doesn't", 'doing', 'done', 'dono', 'dont', "don't", 'doosra', 'doosre', 'down', 'downwards', 'dude', 'dunga', 'dungi', 'during', 'dusra', 'dusre', 'dusri', 'dvaara', 'dvara', 'dwaara', 'dwara', 'each', 'edu', 'eg', 'eight', 'either', 'ek', 'else', 'elsewhere', 'enough', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 'far', 'few', 'fifth', 'fir', 'first', 'five', 'followed', 'following', 'follows', 'for', 'forth', 'four', 'from', 'further', 'furthermore', 'gaya', 'gaye', 'gayi', 'get', 'gets', 'getting', 'ghar', 'given', 'gives', 'go', 'goes', 'going', 'gone', 'good', 'got', 'gotten', 'greetings', 'haan', 'had', 'hadd', 'hadn', 'hadnt', "hadn't", 'hai', 'hain', 'hamara', 'hamare', 'hamari', 'hamne', 'han', 'happens', 'har', 'hardly', 'has', 'hasn', 'hasnt', "hasn't", 'have', 'haven', 'havent', "haven't", 'having', 'he', 'hello', 'help', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', "here's", 'hereupon', 'hers', 'herself', "he's", 'hi', 'him', 'himself', 'his', 'hither', 'hm', 'hmm', 'ho', 'hoga', 'hoge', 'hogi', 'hona', 'honaa', 'hone', 'honge', 'hongi', 'honi', 'hopefully', 'hota', 'hotaa', 'hote', 'hoti', 'how', 'howbeit', 'however', 'hoyenge', 'hoyengi', 'hu', 'hua', 'hue', 'huh', 'hui', 'hum', 'humein', 'humne', 'hun', 'huye', 'huyi', 'i', "i'd", 'idk', 'ie', 'if', "i'll", "i'm", 'imo', 'in', 'inasmuch', 'inc', 'inhe', 'inhi', 'inho', 'inka', 'inkaa', 'inke', 'inki', 'inn', 'inner', 'inse', 'insofar', 'into', 'inward', 'is', 'ise', 'isi', 'iska', 'iskaa', 'iske', 'iski', 'isme', 'isn', 'isne', 'isnt', "isn't", 'iss', 'isse', 'issi', 'isski', 'it', "it'd", "it'll", 'itna', 'itne', 'itni', 'itno', 'its', "it's", 'itself', 'ityaadi', 'ityadi', "i've", 'ja', 'jaa', 'jab', 'jabh', 'jaha', 'jahaan', 'jahan', 'jaisa', 'jaise', 'jaisi', 
 'jo', 'just', 'jyaada', 'jyada', 'k', 'ka', 'karegi', 'karen', 'karenge', 'kari', 'karke', 'karna', 'karne', 'karni', 'karo', 'karta', 'karte', 'karti', 'karu', 'karun', 'karunga', 'karungi', 'kaun', 'kaunsa', 'kayi', 'kch', 'ke', 'keep', 'keeps', 'keh', 'kehte', 'kept', 'khud', 'ki', 'kin', 'kine', 'kinhe', 'kinho', 'kinka', 'kinke', 'kinki', 'kinko', 'kinn', 'kino', 'kis', 'kise', 'kisi', 'kiska', 'kiske', 'kiski', 'kisko', 'kisliye', 'kisne', 'kitna', 'kitne', 'kitni', 'kitno', 'kiya', 'kiye', 'know', 'known', 'knows', 'ko', 'koi', 'kon', 'konsa', 'koyi', 'krna', 'krne', 'kuch', 'kuchch', 'kuchh', 'kul', 'kull', 'kya', 'kyaa', 'kyu', 'kyuki', 'kyun', 'kyunki', 'lagta', 'lagte', 'lagti', 'last', 'lately', 'later', 'le', 'least', 'lekar', 'lekin', 'less', 'lest', 'let', "let's", 'li', 'like', 'liked', 'likely', 'little', 'liya', 'liye', 'll', 'lo', 'log', 'logon', 'lol', 'look', 'looking', 'looks', 'ltd', 'lunga', 'm', 'maan', 'maana', 'maane', 'maani', 'maano', 'magar', 'mai', 'main', 'maine', 'mainly', 'mana', 'mane', 'mani', 'mano', 'many', 'mat', 'may', 'maybe', 'me', 'mean', 'meanwhile', 'mein', 'mera', 'mere', 'merely', 'meri', 'might', 'mightn', 'mightnt', "mightn't", 'mil', 'mjhe', 'more', 'moreover', 'most', 'mostly', 'much', 'mujhe', 'must', 'mustn', 'mustnt', "mustn't", 'my', 'myself', 'na', 'naa', 'naah', 'nahi', 'nahin', 'nai', 'name', 'namely', 'nd', 'ne', 'near', 'nearly', 'necessary', 'neeche', 'need', 'needn', 'neednt', "needn't", 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nhi', 'nine', 'no', 'nobody', 'non', 'none', 'noone', 'nope', 'nor', 'normally', 'not', 'nothing', 'novel', 'now', 'nowhere', 'o', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'own', 'par', 'pata', 'pe', 'pehla', 'pehle', 'pehli', 'people', 'per', 'perhaps', 'phla', 'phle', 'phli', 'placed', 'please', 'plus', 'poora', 'poori', 'provides', 'pura', 'puri', 'q', 'que', 'quite', 'raha', 'rahaa', 'rahe', 'rahi', 'rakh', 'rakha', 'rakhe', 'rakhen', 'rakhi', 'rakho', 'rather', 're', 'really', 'reasonably', 'regarding', 'regardless', 'regards', 'rehte', 'rha', 'rhaa', 'rhe', 'rhi', 'ri', 'right', 's', 'sa', 'saara', 'saare', 'saath', 'sab', 'sabhi', 'sabse', 'sahi', 'said', 'sakta', 'saktaa', 'sakte', 'sakti', 'same', 'sang', 'sara', 'sath', 'saw', 'say', 
'saying', 'says', 'se', 'second', 'secondly', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 'several', 'shall', 'shan', 'shant', "shan't", 'she', "she's", 'should', 'shouldn', 'shouldnt', "shouldn't", "should've", 'si', 'since', 'six', 'so', 'soch', 'some', 'somebody', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'still', 'sub', 'such', 'sup', 'sure', 't', 'tab', 'tabh', 'tak', 'take', 'taken', 'tarah', 'teen', 'teeno', 'teesra', 'teesre', 'teesri', 'tell', 'tends', 'tera', 'tere', 'teri', 'th', 'tha', 'than', 'thank', 
'thanks', 'thanx', 'that', "that'll", 'thats', "that's", 'the', 'theek', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'therefore', 'therein', 'theres', "there's", 'thereupon', 'these', 'they', "they'd", "they'll", "they're", "they've", 'thi', 'thik', 'thing', 'think', 'thinking', 'third', 'this', 'tho', 'thoda', 'thodi', 'thorough', 'thoroughly', 'those', 'though', 'thought', 'three', 'through', 'throughout', 'thru', 'thus', 'tjhe', 'to', 'together', 'toh', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'true', 'truly', 'try', 'trying', 'tu', 'tujhe', 'tum', 'tumhara', 'tumhare', 'tumhari', 'tune', 'twice', 'two', 'um', 'umm', 'un', 'under', 'unhe', 'unhi', 'unho', 'unhone', 'unka', 'unkaa', 'unke', 'unki', 'unko', 'unless', 'unlikely', 'unn', 'unse', 'until', 'unto', 'up', 'upar', 'upon', 'us', 'use', 'used', 'useful', 'uses', 'usi', 'using', 'uska', 'uske', 'usne', 'uss', 'usse', 'ussi', 'usually', 'vaala', 'vaale', 'vaali', 'vahaan', 'vahan', 'vahi', 'vahin', 'vaisa', 'vaise', 'vaisi', 'vala', 'vale', 'vali', 'various', 've', 'very', 'via', 'viz', 'vo', 'waala', 'waale', 'waali', 'wagaira', 'wagairah', 
'wagerah', 'waha', 'wahaan', 'wahan', 'wahi', 'wahin', 'waisa', 'waise', 'waisi', 'wala', 'wale', 'wali', 'want', 'wants', 'was', 'wasn', 'wasnt', "wasn't", 'way', 'we', "we'd", 'well', "we'll", 'went', 'were', "we're", 'weren', 'werent', "weren't", "we've", 'what', 'whatever', "what's", 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', "where's", 'whereupon', 'wherever', 'whether', 'which', 'while', 'who', 'whoever', 'whole', 'whom', "who's", 'whose', 'why', 'will', 'willing', 'with', 'within', 'without', 'wo', 'woh', 'wohi', 'won', 'wont', "won't", 'would', 'wouldn', 'wouldnt', "wouldn't", 'y', 'ya', 'yadi', 'yah', 'yaha', 'yahaan', 'yahan', 'yahi', 'yahin', 'ye', 'yeah', 'yeh', 'yehi', 'yes', 'yet', 'you', "you'd", "you'll", 'your', "you're", 'yours', 'yourself', 'yourselves', "you've", 'yup', 'a', 'ahogy', 'ahol', 'aki', 'akik', 'akkor', 'alatt', 'által', 'általában', 'amely', 'amelyek', 'amelyekben', 'amelyeket', 'amelyet', 'amelynek', 'ami', 'amit', 'amolyan', 'amíg', 'amikor', 'át', 'abban', 'ahhoz', 'annak'
}

def get_parameters(param_key):
    response = ssm.get_parameters(
        Names=[
            param_key,
        ],
        WithDecryption=True
    )
    return response['Parameters'][0]['Value']


# PARAMS IN SSM AWS
temp = get_parameters('/detect-text-trademark/interested_fields')    
INTERESTED_FIELDS = temp.split('+')

temp = get_parameters('/detect-text-trademark/pacing_time_4crawling_mark_type')    
PACING_TIME_4CRAWLING_MARK_TYPE=float(temp)

temp = get_parameters('/detect-text-trademark/violated_char_type_code')    
VIOLATED_CHAR_TYPE_CODES = temp.split(',')[:-1]    # refer to https://trademarkesearch.com/tesssearchhelp.html  - Mark Drawing Code  section

temp = get_parameters('/detect-text-trademark/valid_status_mark_code')
VALID_STATUS_MARK_CODE = temp.split(',')[:-1]  # refer to https://www.uspto.gov/sites/default/files/documents/CSD.pdf 

temp = get_parameters('/detect-text-trademark/length_processed_inputtext')
LENGTH_PROCESSED_INPUT_TEXT = int(temp)

BLACK_LIST_WORDS = set()


print(f"INTERESTED_FIELDS : {INTERESTED_FIELDS} ")
print(f"PACING_TIME_4CRAWLING_MARK_TYPE : ", PACING_TIME_4CRAWLING_MARK_TYPE)
print(f"VIOLATED_CHAR_TYPE_CODES : ", VIOLATED_CHAR_TYPE_CODES)
print(f"VALID_STATUS_MARK_CODE : ", VALID_STATUS_MARK_CODE)
print(f"LENGTH_PROCESSED_INPUT_TEXT : ", LENGTH_PROCESSED_INPUT_TEXT)


def normalize_text(text, num_words = LENGTH_PROCESSED_INPUT_TEXT):
    # remove white spaces
    text = text.lower()
    text = text.replace(',', '')
    text = text.replace('/', '')
    text = text.replace('(', '')
    text = text.replace(')', '')
    text = text.replace('.', '')
    text = text.replace('!', '')
    text = text.replace('-', '')

    text = text.replace('\'', '')

    no_wspace_string = " ".join(text.split()[:num_words])
    print(no_wspace_string)
    return no_wspace_string

def generate_ngrams(words_list, n):
    ngrams_list = []
 
    for num in range(0, len(words_list)):
        ngram = ' '.join(words_list[num:num + n])
        ngrams_list.append(ngram)
 
    return ngrams_list


def word_tokenize(text):
    text = text.lower()
    text = text.replace(',', ' ')
    text = text.replace('/', ' ')
    text = text.replace('(', ' ')
    text = text.replace(')', ' ')
    text = text.replace('.', ' ')
 
    # Convert text string to a list of words
    return text.split()
    
def get_ngrams(text, n):
 
    # text = 'A quick brown fox jumps over the lazy dog.'
 
    words_list = word_tokenize(text)
    ngrams = generate_ngrams(words_list, n)
    return ngrams
    
def get_monogram(input_text):
    text_tokens = word_tokenize(input_text.lower())
    tokens_without_sw = [word for word in text_tokens if not word in STOP_WORDS]
    # print(tokens_without_sw)
    return tokens_without_sw

def save_item_to_DDB(mark_char, serial_no:int, violated:bool, goods_n_service:str, is_status_valid:bool, mark_type_code: str,):
    item = {
          "mark_char": mark_char,
          "serial_no": serial_no,
          "goods_n_service": goods_n_service,
          "is_status_valid": is_status_valid,
          "mark_type_code": mark_type_code,
          "violated": violated
        }
    table.put_item(Item=item)
   
def get_item_from_DDB(mark_char:int):
    response = table.query(
      KeyConditionExpression=Key('mark_char').eq(mark_char)
    )
    return response['Items']
  
def get_found_markchar_sn_list_from_query(query) -> None:
    """
    Generic function to run athena query and ensures it is successfully completed

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
    athena_client = boto3.client("athena", region_name = "ap-southeast-1")
    
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


def preprocess_input(input_text, max_ngram = 5):
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
        
    words_to_check = words_to_check + get_monogram(processed_text)    
    words_to_check = set(words_to_check)    
    words_to_check = {word.upper() for word in words_to_check}
    filter_blacklist_words = set()
    for word in words_to_check:
        if word not in BLACK_LIST_WORDS:
            filter_blacklist_words.add(word)

    print("After removing Black list words: \n", filter_blacklist_words)
    
    return filter_blacklist_words    
  

def check_mark_type_n_usage(items:list):
    """ 
    Param:  items - a list of pair[char_mark, serial_num)]
    Return: 
        violation             : boolean, 
        mark_drawing_type_pair: a list of triple(char_mark, mark_type_code, serial_num)
    """
    
    violation = False
    # violated_mark_char_set = set()  # set of Mark_Char that has code in VIOLATED_CHAR_TYPE_CODES & is_valid_status = True
    mark_drawing_type_pair = [] # list of a pair(char_mark, mark_type_code)
    violated_mark_drawing_type_pair = []
    sn_error_list = []
    for item in items:
        char_mark = item[0]
        serial_no = item[1]
        # if char_mark in violated_mark_char_set:
        #     continue
        print(f"--------\nSTARTING CHECK\nMark: {item[0]}, SN: {item[1]}")
        temp = get_mark_type(serial_no)
        print("Type of temp: ", type(temp))   
        print("TEMP: ", temp)
        if type(temp) is dict: # Web Crawling got error.
            sn_error_list.append(serial_no)
            time.sleep(PACING_TIME_4CRAWLING_MARK_TYPE)
            continue
        mark_type_code, is_valid_status, goods_n_service  = temp[0], temp[1], temp[2]
        mark_drawing_type_pair.append([ char_mark, mark_type_code, serial_no, is_valid_status ])
        if is_valid_status:
            for code in VIOLATED_CHAR_TYPE_CODES:
                if str(code) in mark_type_code :  
                    violation = True
                    violated_mark_drawing_type_pair.append([serial_no, char_mark, mark_type_code, is_valid_status,goods_n_service])
                    # violated_mark_char_set.add(char_mark)

        time.sleep(PACING_TIME_4CRAWLING_MARK_TYPE)
        
    if len(sn_error_list) > 0 :
        print("ERROR WHEN CRAWLING Mark type code , too much request within time !00000000000000000000000000000000000000000000000000000000000000000000000000000000")
        print(sn_error_list)
    else:
        print("Crawling Mark Type Code successfully ! <3   <3   <3")
    print("-------------------------------------------\nVIOLATED Mark char: ")
    print(violated_mark_drawing_type_pair)
    return sn_error_list, violation, violated_mark_drawing_type_pair
    
        
def get_mark_type(serial_no):
    """ 
    Input: 
        serial_no (int)
    Output: 
        [mark_type_code, is_status_valid, goods_n_service ]
            mark_type_code: Mark Drawing Type (there are 7 types in total), e.g: 3 - AN ILLUSTRATION DRAWING WHICH INCLUDES WORD(S)/ LETTER(S)/NUMBER(S)
            is_status_valid: TM5 Common Status (there are 15 status in total), e.g: LIVE/REGISTRATION/Issued and Active 
            goods_n_service: The Goods and Services whose trademark is used for (can be any string), e.g:  Lawn mowers, namely walk-behind lawn mowers and ride-on lawn mowers of less than 12 HP .
    """
    msg = {"serial_no": serial_no, "valid_status_code" : VALID_STATUS_MARK_CODE}
    invoke_response = lambda_client.invoke(FunctionName="CheckMarkType",  # CheckMarkType
                                          InvocationType='RequestResponse', # RequestResponse  Event
                                          Payload=json.dumps(msg))
    print( "Response: ") 
    result = json.load(invoke_response['Payload'])
    status = invoke_response
    print(result)
    print(f"status: \n{status}\n")
    return result
        
        
def extract_text(text):
    response = comprehend_client.detect_entities(Text=text, LanguageCode="en")
    entities = response['Entities']
    res = []
    for ent in entities:
        res.append(ent['Text'])
    print(res)
    
    keyphrase = comprehend_client.detect_key_phrases(Text='string', LanguageCode='en')
    print("Keyphrases: ", keyphrase['KeyPhrases'])
    
    return res
    
    
