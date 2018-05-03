# coding=utf-8
import MySQLdb
from datetime import datetime
import urllib2
from bs4 import BeautifulSoup
from dateutil import parser

#add new event
def add_event(db, cursor, item, event_dic) :
    event_sub       = item['sub']
    event_name      = item['name']
    event_desc      = item['desc']
    date            = str(item['date'])
    date_object     = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    year            = date_object.month
    month           = date_object.year
    date_timestamp  = int(date_object.strftime("%s")) + 25200
    vote            = item['vote']
    date_added      = item['date_added']

    try:
        is_feature = 'yes' if item['is_hot'] else 'no'
        post_name  = event_name.lower().replace(' ', '-').replace("'", '').replace('"', '')

        cursor.execute("INSERT INTO wpll_posts(post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt,  post_status, comment_status, ping_status, post_name, to_ping, pinged, post_modified, post_modified_gmt, post_content_filtered, post_parent, post_type)" +
                       "VALUES(1, NOW(), NOW(), %s, %s, '', 'publish', 'open', 'closed', %s,'', '', NOW(), NOW(), '', 0, 'ajde_events')",
                       (event_desc, event_name, post_name))
    except:
        print(cursor._last_executed)
        raise

    db.commit()

    try :
        id = cursor.lastrowid

        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, 'evcal_srow', %s)", (id, date_timestamp))
        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, 'evcal_erow', %s)", (id, date_timestamp))
        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, 'event_year', %s)", (id, year))
        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, '_event_month', %s)", (id, month))
        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, '_featured', %s)", (id, is_feature))
        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, 'evcal_subtitle', %s)", (id, event_sub))
        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, '_evcal_ec_f1a1_cus', %s)", (id, vote))
        cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                       "VALUES(%s, '_evcal_ec_f3a1_cus', %s)", (id, date_added))
        if (item['is_verify']) :
            cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                           "VALUES(%s, '_evcal_ec_f2a1_cus', %s)", (id, item['is_verify']))

        for key in event_dic:
            cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                           "VALUES(%s, %s, %s)", (id, key, event_dic[key]))
    except:
        print(cursor._last_executed)
        raise

    db.commit()

# update event (vote & is verify)
def update_event(db, cursor, event) :
    try :
        cursor.execute("UPDATE wpll_postmeta SET meta_value = %s " +
                       "WHERE post_id = %s AND meta_key = '_evcal_ec_f1a1_cus' ", (event['vote'], event['id']))

        if (event['is_verify']) :
            cursor.execute("INSERT INTO wpll_postmeta(post_id, meta_key, meta_value)" +
                           "VALUES(%s, '_evcal_ec_f2a1_cus', %s) ON DUPLICATE KEY UPDATE meta_value=%s", (event['id'], event['is_verify'], event['is_verify']))

        cursor.execute("UPDATE wpll_postmeta SET meta_value = %s " +
                       "WHERE post_id = %s AND meta_key = '_evcal_ec_f3a1_cus' ", (event['date_added'], event['id']))
    except:
        print(cursor._last_executed)
        raise

    db.commit()

# crawl event
def crawl_event(db, cursor, event_dic) :
    for i in range(1, 20):
        # specify the url
        quote_page = 'http://coinmarketcal.com/?page=' + str(i)

        # query the website and return the html to the variable ‘page’
        page = urllib2.urlopen(quote_page)

        # parse the html using beautiful soap and store in variable `soup`
        soup = BeautifulSoup(page, 'html.parser')

        article = soup.findAll("article")

        number = 0

        for item in article:
            title       = item.findAll('h5')[2].text.strip()
            description = item.findAll('p', {"class": "description"})[0].text.strip()
            try :
                vote        = item.findAll("span", {"class": "votes"})[0].text.replace('<span class="votes">', '').replace('</span>', '').replace('(', '').replace(')','').replace(' votes', '').replace(' vote', '')
            except:
                vote        = 0
            verify      = item.findAll('i', {"class": "fa-badge-check"})
            is_verify = 1 if verify else 0
            coin      = item.findAll('h5')[1].text.strip().split('(')
            coin_name = coin[0]
            coin_code = coin[1].replace(')', '')
            date_added = item.findAll("p", {"class": "added-date"})[0].text.replace("(Added", "").replace(")", "").strip()
            date_added = datetime.strptime(date_added, '%d %B %Y').strftime('%d/%m/%Y')
            hot = item.findAll('h5')[2].findAll('i', {"class": "glyphicon-fire"})
            is_hot = 1 if hot else 0

            if (is_hot == 1) :
                try:
                    query = "SELECT id " \
                            "FROM wpll_posts  " \
                            "WHERE post_title=%s AND post_content=%s " \
                            "LIMIT 1"
                    cursor.execute(query, (title, description))
                except:
                    print(cursor._last_executed)
                    raise

                value = cursor.fetchone()
                if (value != None and value[0]) :
                    print('Update ' + coin_code + ': ' +title)
                    event = {
                        'id'        : value[0],
                        'vote'      : vote,
                        'is_verify' : is_verify,
                        'date_added': date_added
                    }

                    update_event(db, cursor, event)
                else :
                    print('Add ' + coin_code + ': ' +title)
                    date        = item.findAll('h5')[0].text.strip().replace("By ", "").replace("(or earlier)", "")
                    date        = parser.parse(date)
                    sub         = coin_name + '(' + coin_code + ')'

                    dict = {
                        'sub'       : sub,
                        'name'      : title,
                        'desc'      : description,
                        'is_hot'    : is_hot,
                        'is_verify' : is_verify,
                        'vote'      : vote,
                        'date'      : date,
                        'date_added': date_added
                    }
                    add_event(db, cursor, dict, event_dic)

#main
event_dic = {
    'evcal_allday'                  : 'yes',
    'evcal_event_color'             : '206177',
    'evcal_event_color_n'           : '1',
    'evcal_gmap_gen'                : 'no',
    'evcal_hide_locname'            : 'no',
    'evcal_lmlink_target'           : 'no',
    'evcal_name_over_img'           : 'no',
    'evcal_rep_freq'                : 'daily',
    'evcal_rep_gap'                 : '1',
    'evcal_rep_num'                 : '1',
    'evcal_repeat'                  : 'no',
    'evo_access_control_location'   : 'no',
    'evo_evcrd_field_org'           : 'no',
    'evo_exclude_ev'                : 'no',
    'evo_hide_endtime'              : 'no',
    'evo_repeat_wom'                : '1',
    'evo_span_hidden_end'           : 'no',
    'evo_year_long'                 : 'no',
    'evp_repeat_rb'                 : 'dom',
    'evp_repeat_rb_wk'              : 'sing'
}

db = MySQLdb.connect(host="",    # your host, usually localhost
                     user="",         # your username
                     passwd="",  # your password
                     db="")
cursor = db.cursor()

crawl_event(db, cursor, event_dic)
