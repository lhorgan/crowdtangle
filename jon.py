import ujson as json
import datetime
import math
import os

def process_line(line):
  result = {}
  glitch = False

  try:
    obj = json.loads(line)
  except:
    print("Woops")
    return (False, False)
  url = obj["clean_url"]
  eid = obj["url_rid"]
  #print("OBSERVING " + obj["url_rid"])
  first_observed_post_date = datetime.datetime.max
  fact_check_rating = obj["tpfc_rating"]
  fact_check_time = datetime.datetime.strptime(obj["tpfc_first_fact_check"], "%Y-%m-%dT%H:%M:%S.000Z")
  if obj["first_post_time"] is not None:
    first_post_date = datetime.datetime.strptime(obj["first_post_time"], "%Y-%m-%dT%H:%M:%S.000Z")
  else:
    first_post_date = None
  
  if "ct_response" not in obj or obj["ct_response"] is None:
    print("No ct response for %s" % eid)
    return (False, False)
  if not "result" in obj["ct_response"]:
    return (False, False)
  
  posts = obj["ct_response"]["result"]["posts"]
  domain = obj["full_domain"]


  result = {"shares": {}}  
  result["fact_check_time"] = fact_check_time
  result["domain"] = domain
  result["url"] = url
  result["country"] = "country" 
  result["tpfc_rating"] = fact_check_rating
  result["eid"] = eid

  result["rxns_breakdown"] = []
  result["rxns_sum"] = []

  new_posts = []
  for post in posts:
    if type(post) == type([]):
      for p in post:
        new_posts.append(p)
    else:
      new_posts.append(post)
  posts = new_posts

  if len(posts) == 100:
    glitch = True

  for post in posts:
    result['fact_check_time'] = fact_check_time
    post_date = datetime.datetime.strptime(post["date"], "%Y-%m-%d %H:%M:%S")
    if post_date < first_observed_post_date:
      first_observed_post_date = post_date
    result['shares'][post["id"]] = post_date
    for rxn in post['history']:
      rxn_date = datetime.datetime.strptime(rxn["date"], "%Y-%m-%d %H:%M:%S")
      rxn_count = 0
      rxn_dict = {}
      for rxn_kind in rxn['actual']:
        #print(f"{int(rxn['actual'][rxn_kind])} for {rxn_kind}")
        rxn_count += int(rxn['actual'][rxn_kind])
        rxn_dict[rxn_kind] = int(rxn['actual'][rxn_kind])
      result['rxns_breakdown'].append((rxn_date, rxn_dict))
      result['rxns_sum'].append((rxn_date, rxn_count))
  
  result["first_post_time"] = first_post_date
  result["first_observed_post_time"] = first_observed_post_date

  return (result, glitch)

def write_result(result, f):
  rxn_types = ['sadCount', 'wowCount', 'careCount', 'hahaCount', 'likeCount', 'loveCount', 'angryCount', 'shareCount', 'commentCount', 'thankfulCount']

  share_dates = sorted([result["shares"][share_id] for share_id in result["shares"]])
  if len(share_dates) > 0:
    first_share_date = share_dates[0]
    time_to_fact_check = math.floor((result["fact_check_time"] - first_share_date).total_seconds() / 3600)
    # if time_to_fact_check < 0:
    #   print(f'{eid} {result["fact_check_time"]}\t{first_share_date}\t{share_dates[0]}')
    #   break
    days = {}
    for date in share_dates:
      day = math.floor((date - first_share_date).total_seconds() / 86400)
      if day not in days:
        days[day] = [0, 0, {}]
      days[day][0] += 1
    
    for rxn_date, rxn_count in result['rxns_sum']:
      rxn_day = math.floor((rxn_date - first_share_date).total_seconds() / 86400)
      if rxn_day not in days:
        days[rxn_day] = [0, 0, {}]
      days[rxn_day][1] += rxn_count
    
    for rxn_date, rxn_breakdown in result['rxns_breakdown']:
      rxn_day = math.floor((rxn_date - first_share_date).total_seconds() / 86400)
      #print("RXN DAY IS " + str(rxn_day))
      for rxn_type in rxn_breakdown:
        if rxn_type not in days[rxn_day][2]:
          days[rxn_day][2][rxn_type] = 0
        days[rxn_day][2][rxn_type] += rxn_breakdown[rxn_type]
      #days[rxn_day][2] = rxn_breakdown
    
    days_array = []
    for day in days:
      days_array.append([day, days[day]]) # [day #, shares]
    days_array = sorted(days_array, key=lambda x: x[0])
    
    for day,response in days_array:
      shares,rxns,rxns_breakdown = response
      #print(str(rxns) + ", " + str(rxns_breakdown))
      line_to_write = f'{result["eid"]}\t{day}\t{shares}\t{rxns}\t{result["tpfc_rating"]}\t{time_to_fact_check}\t{result["domain"]}\t{result["url"]}\t{result["country"]}\t{result["first_post_time"]}\t{result["first_observed_post_time"]}'
      #line_to_write = f'{eid}\t{day}\t{shares}\t{rxns}\t{result["tpfc_rating"]}\t'
      for rxn_type in rxn_types:
        print(rxns_breakdown)
        if rxn_type in rxns_breakdown:
          if rxn_type == "thankfulCount" and rxns_breakdown[rxn_type] > 0:
            print(f"{rxn_type} {rxns_breakdown[rxn_type]}")
          line_to_write += (f"{int(rxns_breakdown[rxn_type])}\t")
        else:
          #print("We haven't got " + rxn_type)
          line_to_write += (f"0\t")
      line_to_write += "\n"
      f.write(line_to_write)

def go():
  data = {}
  rxn_types = ['sadCount', 'wowCount', 'careCount', 'hahaCount', 'likeCount', 'loveCount', 'angryCount', 'shareCount', 'commentCount', 'thankfulCount']


  try:
    os.remove("./results_test.tsv")
    print("REMOVED!")
  except:
    pass

  with open("./ss1_fact_checking.tsv") as f:
    line = f.readline()
    while line:
      [eid, url, full_domain, parent_domain, datestamp, timestamp, 
      headline, summary, tpfc_rating, tpfc_datestamp, tpfc_timestamp, 
      _, _, _, country] = line.strip().split("\t")
      if eid in data:
        data[eid]["country"] = country
        data[eid]["tpfc_rating"] = tpfc_rating
      line = f.readline()

  processed_eids = set()
  glitches = []
  with open("/home/luke/bfd/ct_data_100.json") as f:
    line = f.readline()
    i = 0
    with open("results_test.tsv", "a+") as results_file:
      line_to_write = f'eid\tday\tshares\treactions\tfact_check_rating\tfact_check_time\tdomain\turl\tcountry\tfirst_post_time\tfirst_observed_post_time\t'
      for rxn_type in rxn_types:
        line_to_write += f"{rxn_type}\t"
      line_to_write += "\n"
      results_file.write(line_to_write)

      while line and i < 1: #float("inf"):
        print(i)
        i += 1
        result, glitch = process_line(line)
        if glitch:
          glitches.append((result["eid"], i))
        if result:
          #print("\n\n")
          processed_eids.add(eid)
          write_result(result, results_file)
        line = f.readline()
  
  for glitch in glitches:
    print(glitch)

  with open("./ct_data.json") as f:
    line = f.readline()
    i = 0
    with open("results_test.tsv", "a+") as results_file:
      while line and i < 0:#float("inf"):
        i += 1
        result, glitch = process_line(line)
        if result:
          if result["eid"] not in processed_eids:
            write_result(result, results_file)
        line = f.readline()

go()