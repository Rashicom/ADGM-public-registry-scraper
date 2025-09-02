import requests
import json
import os
import csv
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from formatter import fieldnames

def get_csv_row_count(filename="result.csv"):
    """Gets the number of data rows in a CSV file (excluding the header)."""
    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        return 0
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Count rows, subtract 1 for the header
            row_count = sum(1 for row in reader) - 1
            return max(0, row_count)  # Ensure count is not negative
    except Exception as e:
        print(f"Could not read {filename}: {e}")
        return 0


# static data
aura_context_data = {
    "mode": "PROD",
    "fwuid": "eE5UbjZPdVlRT3M0d0xtOXc5MzVOQWg5TGxiTHU3MEQ5RnBMM0VzVXc1cmcxMi42MjkxNDU2LjE2Nzc3MjE2",
    "app": "siteforce:communityApp",
    "loaded": {"APPLICATION@markup://siteforce:communityApp": "1301_LBgf00TjwltnPu835uHgpg"},
    "dn": [],
    "globals": {},
    "uad": True
}

def get_company_ids(page = 1, page_size = 10):
    search_string = "{\"advancedSearch\":[{\"fieldSetName\":\"SearchFieldsAdvGen\",\"headers\":[{\"dataType\":\"BOOLEAN\",\"fieldAPIName\":\"Is_continued__c\",\"isRequired\":false,\"label\":\"Is continued?\",\"options\":[],\"value\":\"\",\"allowRender\":true},{\"dataType\":\"PICKLIST\",\"fieldAPIName\":\"Entity_Status__c\",\"isRequired\":false,\"label\":\"Entity Status\",\"options\":[],\"value\":\"\",\"allowRender\":true},{\"dataType\":\"DATE\",\"fieldAPIName\":\"Incorporation_Date__c\",\"isRequired\":false,\"label\":\"Incorporation Date\",\"options\":[],\"value\":\"\",\"allowRender\":false},{\"dataType\":\"PICKLIST\",\"fieldAPIName\":\"Category__c\",\"isRequired\":false,\"label\":\"Category\",\"options\":[],\"value\":\"\",\"allowRender\":true}],\"isParent\":true,\"objectName\":\"Account\"}],\"advancedSearch_auditors\":[{\"fieldSetName\":\"SearchFieldsAuditor\",\"headers\":[],\"isParent\":false,\"objectName\":\"Account\",\"relationshipName\":\"Subject_Account__r\"}],\"advancedSearch_companies\":[{\"fieldSetName\":\"SearchFieldsAdvComp\",\"headers\":[],\"isParent\":true,\"objectName\":\"Account\"}],\"advancedSearch_foliostrataplanstratalot\":[{\"fieldSetName\":\"SearchFieldsFolioStrataPlanLotAdvanced\",\"headers\":[],\"isParent\":false,\"objectName\":\"Property__c\"}],\"advancedSearch_foundation\":[{\"fieldSetName\":\"SearchFieldsAdvFoun\",\"headers\":[],\"isParent\":true,\"objectName\":\"Account\"}],\"advancedSearch_general\":[{\"fieldSetName\":\"SearchFieldsAdvGen\",\"headers\":[{\"dataType\":\"BOOLEAN\",\"fieldAPIName\":\"Is_continued__c\",\"isRequired\":false,\"label\":\"Is continued?\",\"options\":[],\"value\":\"\"},{\"dataType\":\"PICKLIST\",\"fieldAPIName\":\"Entity_Status__c\",\"isRequired\":false,\"label\":\"Entity Status\",\"options\":[],\"value\":\"\"},{\"dataType\":\"DATE\",\"fieldAPIName\":\"Incorporation_Date__c\",\"isRequired\":false,\"label\":\"Incorporation Date\",\"options\":[],\"value\":\"\"},{\"dataType\":\"PICKLIST\",\"fieldAPIName\":\"Category__c\",\"isRequired\":false,\"label\":\"Category\",\"options\":[],\"value\":\"\"}],\"isParent\":true,\"objectName\":\"Account\"}],\"advancedSearch_InsolvencyPractitioner\":[{\"fieldSetName\":\"SearchFieldsInsolvencyPractitioner\",\"headers\":[],\"isParent\":false,\"objectName\":\"Account\",\"relationshipName\":\"Subject_Account__r\"}],\"advancedSearch_partnership\":[{\"fieldSetName\":\"SearchFieldsAdvPart\",\"headers\":[],\"isParent\":true,\"objectName\":\"Account\"}],\"advancedsearch_RegisteredBuilding\":[{\"fieldSetName\":\"SearchFieldsLeaseAdvancedBuilding\",\"headers\":[],\"isParent\":false,\"objectName\":\"Linked_Unit__c\"}],\"advancedsearch_RegisteredLand\":[{\"fieldSetName\":\"SearchFieldsLeaseAdvancedLand\",\"headers\":[],\"isParent\":false,\"objectName\":\"Linked_Unit__c\"}],\"advancedsearch_RegisteredLease\":[{\"fieldSetName\":\"SearchFieldsLeaseAdvanced\",\"headers\":[],\"isParent\":false,\"objectName\":\"Linked_Unit__c\"}],\"advancedsearch_RegisteredUnit\":[{\"fieldSetName\":\"SearchFieldsLeaseAdvancedUnit\",\"headers\":[],\"isParent\":false,\"objectName\":\"Linked_Unit__c\"}],\"advancedSearch_reservedname\":[{\"fieldSetName\":\"ReservedName\",\"headers\":[],\"isParent\":true,\"objectName\":\"Trade_Name__c\"}],\"advancedSearch_role\":[{\"fieldSetName\":\"RoleSearchFields\",\"headers\":[],\"isParent\":true,\"objectName\":\"Role__c\"},{\"fieldSetName\":\"RoleFieldSet\",\"headers\":[],\"isParent\":false,\"objectName\":\"Account\",\"relationshipName\":\"Subject_Account__r\"}],\"advancedSearch_temporarypermit\":[{\"fieldSetName\":\"TempPermitSearchFields\",\"headers\":[],\"isParent\":true,\"objectName\":\"Account\"}],\"buttonConfig\":{\"buttonPlacement\":\"RIGHT\",\"buttons\":[{\"actionType\":\"Create_BookMark\",\"label\":\"Add to Watchlist\",\"renderCheckField\":\"Show_Request_Option__c\",\"renderCheckValue\":\"Add BookMark\",\"styleClass\":\"requestBtn\"},{\"actionType\":\"Remove_BookMark\",\"label\":\"Remove from Watchlist\",\"renderCheckField\":\"Show_Request_Option__c\",\"renderCheckValue\":\"Remove BookMark\",\"styleClass\":\"cancelBtn\"}],\"canSelectMultiple\":false,\"rowLevel\":true},\"defaultOrderBy\":\"ASC\",\"generalSearch\":[{\"fieldSetName\":\"SearchFields\",\"headers\":[{\"dataType\":\"STRING\",\"fieldAPIName\":\"Name\",\"isRequired\":false,\"label\":\"Account Name\",\"options\":[],\"value\":\"\"}],\"isParent\":true,\"objectName\":\"Account\"}],\"generalSearch_Folio\":[{\"fieldSetName\":\"SearchFieldsFolioGeneral\",\"headers\":[],\"isParent\":true,\"objectName\":\"Property__c\"}],\"generalsearch_RegisteredLease\":[{\"fieldSetName\":\"SearchFieldsLeaseGeneral\",\"headers\":[],\"isParent\":true,\"objectName\":\"Linked_Unit__c\"}],\"generalSearch_StrataLot\":[{\"fieldSetName\":\"SearchFieldsStrataLotGeneral\",\"headers\":[],\"isParent\":true,\"objectName\":\"Property__c\"}],\"generalSearch_StrataPlan\":[{\"fieldSetName\":\"SearchFieldsStrataPlanGeneral\",\"headers\":[],\"isParent\":true,\"objectName\":\"Property__c\"}],\"orderByFields\":\"Name\",\"resultFieldSet\":\"RequestAccessSearchResult\",\"showAdvancedSearch\":false,\"showRegisteredEntities\":true,\"pageNumber\":1,\"pageSize\":\"10\"}"
    search_json = json.loads(search_string)
    search_json["pageNumber"] = page
    search_json["pageSize"] = page_size
    message_data = {
    "actions": [
            {
                "id": "149;a",
                "descriptor": "aura://ApexActionController/ACTION$execute",
                "callingDescriptor": "UNKNOWN",
                "params": {
                    "namespace": "",
                    "classname": "RASearchUtil",
                    "method": "getSearchResponseForPR",
                    "params": {
                        "jsonSearchString": json.dumps(search_json),
                        "isAdvancedSearch": True,
                        "searchMetadataProcessName": "Public Registry Search",
                        "registerType": "",
                        "registerationDate": "[{\"fieldAPIName\":\"Incorporation_Date__c\",\"value\":\"\",\"dataType\":\"DATE\"},{\"fieldAPIName\":\"DateOperator\",\"value\":\"=\",\"dataType\":\"PICKLIST\"},{\"fieldAPIName\":\"Incorporation_Date__c\",\"value\":\"\",\"dataType\":\"DATE\"}]"
                    },
                    "cacheable": False,
                    "isContinuation": False
                }
            }
        ]
    }

    payload = {
        "aura.context": json.dumps(aura_context_data),
        "aura.token":"null",
        "message":json.dumps(message_data),
    }
    try:
        resp = requests.post(
            "https://newreg.adgm.com/s/sfsites/aura",
            data=payload
        )
        if resp.status_code != 200:
            raise Exception(f"Cannot get companie ids : unexpected response status code. Error : {resp.text}")
    except Exception as e:
        raise Exception(f"Cannot get companie ids, page: {page}, page_size: {page_size}")
    
    companies = resp.json().get("actions")[0].get("returnValue").get("returnValue").get("data").get("data")
    return [comp.get("Id") for comp in companies]
    


# get trade names
def retrieve_data(company_id):
    msg = {
   "actions":[
       # genral details
       {
            "id":"316;a",
            "descriptor":"apex://RAPRPageFlowController/ACTION$getPageDetails",
            "callingDescriptor":"markup://c:rAPageFlowPublicRegistrar",
            "params":{
                "pageFlowId":None,
                "recordId":company_id
            }
        },
        # business activities
        {
             "id":"695;a",
             "descriptor":"apex://RAPRPageFlowController/ACTION$getPageAndRelatedDetails",
             "callingDescriptor":"markup://c:rAPageFlowPublicRegistrar",
             "params":{
                "pageId":"a0z5q000000kSiyAAE",
                "recordId":company_id
             }
        },
        # trade names    
        {
           "id":"710;a",
           "descriptor":"apex://RAPRPageFlowController/ACTION$getPageAndRelatedDetails",
           "callingDescriptor":"markup://c:rAPageFlowPublicRegistrar",
           "params":{
              "pageId":"a0z5q000000kSizAAE",
              "recordId":company_id
           }
        },
        # address
        {
          "id":"908;a",
          "descriptor":"apex://RAPRPageFlowController/ACTION$getPageAndRelatedDetails",
          "callingDescriptor":"markup://c:rAPageFlowPublicRegistrar",
          "params":{
             "pageId":"a0z5q000000kSj0AAE",
             "recordId":company_id,
          }
       },
       # shareholder
        {
          "id":"1634;a",
          "descriptor":"apex://RAPRPageFlowController/ACTION$getPageAndRelatedDetails",
          "callingDescriptor":"markup://c:rAPageFlowPublicRegistrar",
          "params":{
             "pageId":"a0z5q000000kSj2AAE",
             "recordId":company_id,
          }
       },
       # directors
        {
          "id":"1702;a",
          "descriptor":"apex://RAPRPageFlowController/ACTION$getPageAndRelatedDetails",
          "callingDescriptor":"markup://c:rAPageFlowPublicRegistrar",
          "params":{
             "pageId":"a0z5q000000kSj8AAE",
             "recordId":company_id,
          }
       },
       # secretary
       {
         "id":"1732;a",
         "descriptor":"apex://RAPRPageFlowController/ACTION$getPageAndRelatedDetails",
         "callingDescriptor":"markup://c:rAPageFlowPublicRegistrar",
         "params":{
            "pageId":"a0z5q000000kSj9AAE",
            "recordId":company_id,
         }
       },
       
    ]
}
    payload = {
        "aura.context": json.dumps(aura_context_data),
        "aura.token":"null",
        "message":json.dumps(msg),
    }
    try:
        resp = requests.post(
            "https://newreg.adgm.com/s/sfsites/aura",
            data=payload
        )
        if resp.status_code != 200:
            raise Exception(f"Cannot get company address : unexpected response status code. Error : {resp.text}")
    except Exception as e:
        raise Exception(f"Cannot get company address, company_id: {company_id}")
    return resp.json().get("actions")



def batch_fetch_campany(company_ids):
    results = []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(retrieve_data, cid): cid for cid in company_ids}
        for future in tqdm(as_completed(futures), total=len(company_ids), desc="Scrapping company data"):
            cid = futures[future]
            try:
                data = future.result()
                tqdm.write(f"✅ Retrieved data for company ID: {cid}")
                results.append(data)
            except Exception as e:
                tqdm.write(f"❌ Failed to retrieve data for company ID: {cid} — {e}")
    return results



def write_to_csv(data,file_name="result.csv"):
    data_to_write = []
    for doc in data:
        genral_details = doc[0].get("returnValue").get("data").get("mpObjectsData").get("Account")[0]
        business_activities = doc[1].get("returnValue").get("data").get("mpObjectTompRecordToActiveInActiveRecords")[0].get("activeInactiveRecords")
        trade_names = doc[2].get("returnValue").get("data").get("mpObjectTompRecordToActiveInActiveRecords")[0].get("activeInactiveRecords")
        address = doc[3].get("returnValue").get("data").get("mpObjectTompRecordToActiveInActiveRecords")[0].get("activeInactiveRecords") 
        shareholders = doc[4].get("returnValue").get("data").get("mpObjectTompRecordToActiveInActiveRecords")[0].get("activeInactiveRecords") 
        directors = doc[5].get("returnValue").get("data").get("mpObjectTompRecordToActiveInActiveRecords")[0].get("activeInactiveRecords") 
        secretary = doc[6].get("returnValue").get("data").get("mpObjectTompRecordToActiveInActiveRecords")[0].get("activeInactiveRecords")

        
        recc = {
            # genral details
            "Entity_Name": genral_details.get("Name"),
            "Entity_Type": genral_details.get("Entity_Type__c"),
            "Entity_Subtype": genral_details.get("Entity_Sub_Type__c"),
            "Entity_Status": genral_details.get("Entity_Status__c"),
            "Incorporation_Date": genral_details.get("Incorporation_Date__c"),
            "License_Status": genral_details.get("License_Status__c"),
            "License_Expiry_Date": genral_details.get("License_Expiry_Date__c"),
            "Corporate_Service_Provider": genral_details.get("CSP_Name__c"),
            "Confirmation_Statement_last_filed_on": genral_details.get("Confirmation_Statement_last_filed_date__c"),
            "Accounting_Reference_Date": genral_details.get("New_Accounting_Reference_Date__c"),
            # additional genral details
            "Category__c": genral_details.get("Category__c"),
            "Invoice_Count__c": genral_details.get("Invoice_Count__c"),
            "Amount_of_Authorised_Share_Capital__c": genral_details.get("Amount_of_Authorised_Share_Capital__c"),
            "Registration_Number__c": str(genral_details.get("Registration_Number__c")),
            "Document_Name__c": genral_details.get("Document_Name__c"),
            "License_Public_Link__c": genral_details.get("License_Public_Link__c"),
            "Total_Number_of_Issued_Share__c": genral_details.get("Total_Number_of_Issued_Share__c"),
            "Total_Number_of_issued_Share_FT__c": genral_details.get("Total_Number_of_issued_Share_FT__c"),
    
        }
        
        # update business activity
        business_activity_list = []
        for rec in business_activities:
            business_activity_list.append(
                " | ".join
                (
                    f"Type:{record.get("Type__c")}, Code:{record.get("Actual_Activity_code__c")} Section:{record.get("Section__c")}, Activities:{record.get("Activity_Name__c")}"
                    for record in rec.get("records")
                )
            )
        recc["Business_Activity"] = " || ".join(act for act in business_activity_list)

        # update trade names
        tradename_list = []
        for rec in trade_names:
            tradename_list.append(
                " | ".join
                (
                    f"Trade Name:{record.get("Name_in_English__c")}, Trade Name Became Effective on:{record.get("Effective_Date__c")}"
                    for record in rec.get("records")
                )
            )
        recc["Trade_Names"] = " || ".join(trd for trd in tradename_list)

        # update address
        address_list = []
        for rec in address:
            address_list.append(
                " | ".join(
                    f"{record.get("Full_Address__c")}"
                    for record in rec.get("records")
                )
            )
        recc["Addresses"] = " || ".join(addr for addr in address_list)
        

        # update Shareholders
        shareholders_list = []
        for rec in shareholders:
            shareholders_list.append(
                " | ".join(
                    f"Name : {record.get("Role_Full_Name__c")}, Appointment Date : {record.get("Appointment_Date__c")}"
                    for record in rec.get("records")
                )
            )
        recc["Shareholder"] = " || ".join(shr for shr in shareholders_list)

        # update Director
        directos_list = []
        for rec in directors:
            directos_list.append(
                " | ".join(
                    f"Name : {record.get("Role_Full_Name__c")}, Appointment Date : {record.get("Appointment_Date__c")}"
                    for record in rec.get("records")
                )
            )
        recc["Director"] = " || ".join(dr for dr in directos_list)

        # update Secretary
        secretary_list = []
        for rec in secretary:
            secretary_list.append(
                " | ".join(
                    f"Name : {record.get("Role_Full_Name__c")}, Appointment Date : {record.get("Appointment_Date__c")}"
                    for record in rec.get("records")
                )
            )
        recc["Secretary"] = " || ".join(dr for dr in secretary_list)
    
        data_to_write.append(recc)
        
    
    # Check if the file is new or empty to decide whether to write the header.
    write_header = not os.path.exists(file_name) or os.path.getsize(file_name) == 0

    try:
        with open(file_name, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            if write_header:
                writer.writeheader()

            writer.writerows(data_to_write)
            print(f"Successfully wrote {len(data)} rows to {file_name}")
    except IOError as e:
        print(f"I/O error while writing to {file_name}: {e}")


