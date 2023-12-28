import json
import pm4py
from pm4py.objects.conversion.log import converter as log_converter
from kafka import KafkaProducer


class AlphabetService:
    def __init__(self):
        self.activity_to_alphabet = {}
        self.alphabet_to_activity = {}
        self.char_counter = 64  

    def alphabetize(self, label):
        if label not in self.activity_to_alphabet:
            self.char_counter += 1
            self.activity_to_alphabet[label] = chr(self.char_counter)
            self.alphabet_to_activity[chr(self.char_counter)] = label
            #print("label",label,"char",chr(self.char_counter))
        return self.activity_to_alphabet[label]

    def clear(self):
        self.activity_to_alphabet.clear()
        self.alphabet_to_activity.clear()
        self.char_counter = 64




def process_trace(trace, nodes, current_node='root'):
    last_node = current_node #root

    for event in trace:
        event_name = event['alphabetized_label']
        node_id = f"{last_node}-{event_name}"

        if node_id not in nodes:
            nodes[node_id] = {'label': event_name, 'parent': last_node if last_node != 'root' else None, 'children': set()}

        #update the children of the parent node
        if last_node != 'root':
            nodes[last_node]['children'].add(node_id)

        last_node = node_id

    return nodes

def convert_sets_to_lists(obj):
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: convert_sets_to_lists(v) for k, v in obj.items()}
    #elif isinstance(obj, list):
    #    return [convert_sets_to_lists(x) for x in obj]
    else:
        return obj

event_log = pm4py.read_xes("BPI_2012_1k_sample.xes")
dataframe = log_converter.apply(event_log, variant=log_converter.Variants.TO_DATA_FRAME)
labels_trace = dataframe[["concept:name", "case:concept:name"]]
grouped_traces = labels_trace.groupby("case:concept:name",sort=False)


first_three_keys = labels_trace["case:concept:name"].unique()[:3]
filtered_df = labels_trace[labels_trace["case:concept:name"].isin(first_three_keys)]
grouped_traces = filtered_df.groupby("case:concept:name", sort=False)


alphabet_service = AlphabetService()
nodes = {}
for trace, group in grouped_traces:
    group['alphabetized_label'] = group["concept:name"].apply(alphabet_service.alphabetize)
    #print(group.to_dict("records"))
    nodes = process_trace(group.to_dict('records'), nodes)


producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
    key_serializer=str.encode)
topic_name = "test_label_topic"

labels = alphabet_service.activity_to_alphabet
for event, label in labels.items():
    message = json.dumps({"event": event, "label": label}).encode("utf-8")
    producer.send(topic_name,key=event ,value=message)

topic_name = "test_xes_topic"
for node_id, node_data in nodes.items():
    message = {'node_id': node_id, **node_data}
    #i need to do this since sets are not serializable but i want the uniqueness of using a set
    #initial quick solution, can change later if too slow
    message = convert_sets_to_lists(message)
    key = message["node_id"]
    message_bytes = json.dumps(message).encode('utf-8')
    producer.send(topic_name,key=key ,value=message_bytes)

producer.flush()
producer.close()
    

