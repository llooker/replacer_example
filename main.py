# regex not needed, but as promised here is the pattern:
# import re
# mustachePattern = r'\$\{([a-z\._0-9]*)\}'
import configparser
import json
from looker_sdk import models40 as looker_types, init40 as Looker

CONFIG_FILE = 'credentials.ini'
config = configparser.ConfigParser()
config.read(CONFIG_FILE)


field_to_be_replaced = 'order_items.sale_price'
new_field = 'order_items.sale_price_2'

filter_expression = \
f"contains(${{query.dynamic_fields}},\"{field_to_be_replaced}\")\n"+\
f"OR contains(${{query.filters}},\"{field_to_be_replaced}\")\n"+\
f"OR contains(${{query.fields}},\"{field_to_be_replaced}\")\n"+\
f"OR contains(${{query.pivots}},\"{field_to_be_replaced}\")\n"+\
f"OR contains(${{query.dynamic_fields}},\"{field_to_be_replaced}\")"

query_for_looks = {
	"view": "look",
	"fields": ["look.id"],
	"filter_expression": filter_expression,
	"model": "system__activity"
}
query_for_dashboards = {
	"view": "dashboard",
	"fields": ["dashboard_element.id"],
	"filter_expression": filter_expression,
	"model": "system__activity"
}

looker = Looker(config_file=CONFIG_FILE,section='myinstance')


#search the system activity to find those looks containing a field reference
matching_look_ids = [str(row['look.id']) for row in json.loads(looker.run_inline_query(result_format='json',body=query_for_looks))]

#search the system activity to find those dashboards with elements containing a field reference
matching_dashboard_element_ids = [
                            str(row['dashboard_element.id']) 
                            for row in 
                                json.loads(
                                    looker.run_inline_query(result_format='json',
                                                            body=query_for_dashboards)
                                                        )]


def replace_list_item(items:list) -> list:
    if items: return list(map(
                                lambda item: item.replace(field_to_be_replaced, new_field),
                                items
                            ))
    else: return list()

def replace_dict_keys(old_data:dict) -> dict:
    if old_data:
        if field_to_be_replaced in old_data.keys():
            old_data.update({
                new_field: old_data.pop(field_to_be_replaced)
            })
        return old_data
    else: return dict()

def replace_string(subject:str) -> str:
    if subject:
        return subject.replace(field_to_be_replaced,new_field)
    else: return subject

def recursive_replacer(data: dict) -> dict:
    if data:
        for k,v in data.items():
            if type(v) == str:
                data[k] = replace_string(v)
            elif type(v) == list:
                if len(v) > 0:
                    if type(v[0]) == str:
                        data[k] = replace_list_item(v)
                    elif type(v[0]) == dict:
                        data[k] = [recursive_replacer(i) for i in v]
            elif type(v) == dict:
                data[k] = recursive_replacer(v)
    return {}


def replacer(old_query:looker_types.Query) -> looker_types.Query:
    '''takes a query, performs substitutions & returns the new query id'''
    new_query = looker.create_query(body={
        "model": old_query.model,
        "view": old_query.view,
        "fields": replace_list_item(old_query.fields),
        "pivots": replace_list_item(old_query.pivots),
        "fill_fields": replace_list_item(old_query.fill_fields),
        "filters": replace_dict_keys(old_query.filters),
        "filter_expression": replace_string(old_query.filter_expression),
        "sorts": replace_list_item(old_query.sorts),
        "limit": old_query.limit,
        "column_limit": old_query.column_limit,
        "total": old_query.total,
        "row_total": old_query.row_total,
        "subtotals": replace_list_item(old_query.subtotals),
        "vis_config": recursive_replacer(old_query.vis_config),
        "dynamic_fields": replace_string(old_query.dynamic_fields)
        })
    return new_query

#Loop through look_ids and perform replacement
for look_id in matching_look_ids:
    look = looker.look(look_id=look_id)
    nq = replacer(look.query)
    looker.update_look(
            look_id=look.id,
            body={'query_id':nq.id}
                )

#Loop through dashboard elements and perform replacement
for dashboard_element_id in matching_dashboard_element_ids:
    element = looker.dashboard_element(dashboard_element_id)
    nq = replacer(element.query)
    looker.update_dashboard_element(dashboard_element_id=element.id,body={
            'query_id':nq.id
        })
