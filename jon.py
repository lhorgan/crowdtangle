import ujson as json
import datetime
import math

def go():
  post_ids = set()
  data = {}

  with open("/home/luke/bfd/ct_data_100.json") as f:
    line = f.readline()
    i = 0
    while line:
      print(i)
      i += 1
      try:
        obj = json.loads(line)
        url = obj["clean_url"]
        eid = obj["url_rid"]
        # if(eid == "3k5wgti81z22ez0"):
        #   print("WRITING")
        #   YAY = open("YAY.json", "w+")
        #   YAY.write(line)
        #   YAY.close()
        fact_check_rating = obj["tpfc_rating"]
        fact_check_time = datetime.datetime.strptime(obj["tpfc_first_fact_check"], "%Y-%m-%dT%H:%M:%S.000Z")
        first_post_date = datetime.datetime.strptime(obj["first_post_time"], "%Y-%m-%dT%H:%M:%S.000Z")
        if not "result" in obj["ct_response"]:
          # "internal error occurred, please try again later or reach out to your CrowdTangle Representative."
          line = f.readline()
          continue
        posts = obj["ct_response"]["result"]["posts"]
        #print(len(posts))
        domain = obj["full_domain"]

        if eid not in data:
          data[eid] = {"shares": {}}
        #print(f'{url}: {len(posts)}')
        data[eid]["fact_check_time"] = fact_check_time
        data[eid]["domain"] = domain
        data[eid]["url"] = url
      
        data[eid]["country"] = "country" 
        data[eid]["tpfc_rating"] = "false"
        data[eid]["rxns"] = []

        for post in posts:
          data[eid]['fact_check_time'] = fact_check_time
          #data[eid]['fact_check_rating'] = fact_check_rating
          post_date = datetime.datetime.strptime(post["date"], "%Y-%m-%d %H:%M:%S")
          data[eid]['shares'][post["id"]] = post_date
          for rxn in post['history']:
            rxn_date = datetime.datetime.strptime(rxn["date"], "%Y-%m-%d %H:%M:%S")
            rxn_count = 0
            for rxn_kind in rxn['actual']:
              rxn_count += int(rxn['actual'][rxn_kind])
            data[eid]['rxns'].append((rxn_date, rxn_count))

          day = math.floor((post_date - first_post_date).total_seconds() / 86400)
          #print(f'{first_post_date}\t{post_date}\t{day}')
          #print(day)
        # except:
        #   print("Couldn't load the line")
        line = f.readline()
      except:
        line = f.readline()
  
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

  f = open("/home/luke/bfd/results_rxns_100.tsv", "a+")
  line_to_write = f'eid\tday\tshares\treactions\tfact_check_rating\tfact_check_time\tdomain\turl\tcountry\n'
  f.write(line_to_write)
  for eid in data:
    if True:
      share_dates = sorted([data[eid]["shares"][share_id] for share_id in data[eid]["shares"]])
      if len(share_dates) > 0:
        first_share_date = share_dates[0]
        time_to_fact_check = math.floor((data[eid]["fact_check_time"] - first_share_date).total_seconds() / 3600)
        # if time_to_fact_check < 0:
        #   print(f'{eid} {data[eid]["fact_check_time"]}\t{first_share_date}\t{share_dates[0]}')
        #   break
        days = {}
        for date in share_dates:
          day = math.floor((date - first_share_date).total_seconds() / 86400)
          if day == 137:
            print("DAY IS 137")
            print(eid)
          if day not in days:
            days[day] = [0, 0]
          if day == 137:
            print(f"incrementing day from {days[day][0]} to {days[day][0]+1}\n\n")
          days[day][0] += 1
        
        for rxn_date, rxn_count in data[eid]['rxns']:
          rxn_day = math.floor((rxn_date - first_share_date).total_seconds() / 86400)
          if rxn_day not in days:
            days[rxn_day] = [0, 0]
          days[rxn_day][1] += 1
        
        days_array = []
        for day in days:
          days_array.append([day, days[day]]) # [day #, shares]
        days_array = sorted(days_array, key=lambda x: x[0])
        
        for day,response in days_array:
          shares,rxns = response
          line_to_write = f'{eid}\t{day}\t{shares}\t{rxns}\t{data[eid]["tpfc_rating"]}\t{time_to_fact_check}\t{data[eid]["domain"]}\t{data[eid]["url"]}\t{data[eid]["country"]}\n'
          f.write(line_to_write)
          #print(line_to_write)
  f.close()

go()