import networkx as nx
from server.storage import nosql_db
from collections import Counter
import re
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def normalize_node_text(text):
    text = text.lower()
    # add this back when we know how to handle - in the db query
    # text = text.replace("-", " ")
    text = re.sub(r"\.$", "", text)
    text = re.sub(r"s$", "", text)
    return text


def get_edge_topic_facts(field_id):
    return []

def get_node_field_graph_json(topic_facts):
    G = nx.Graph()
    answers = []
    for f in topic_facts:
        if f["answer"]:
            answers.append(f["answer"])
    c = Counter(answers)
    for key, value in c.most_common()[0:50]:
        G.add_node(key, size=value, label=key)

    return nx.json_graph.node_link_data(G)


def get_triple_field_graph(topic_facts):
    G = nx.DiGraph()
    max_count = 10000
    count = 0
    for f in topic_facts:
        head = f["relation_head"]
        tail = f["relation_tail"]
        if head and tail and head !=tail:
            if G.has_edge(head, tail):
                G.add_edge(head, tail, size=G[head][tail]["size"] + 1)
            else:
                G.add_node(head, label=head)
                G.add_node(tail, label=tail)
                G.add_edge(head, tail, size=1)
                count = count + 1
                if count > max_count:
                    break

    def filter_node(n1):
        return nx.degree(G, n1) >= 2

    view = nx.subgraph_view(G, filter_node=filter_node)
    return view


def get_triple_field_graph_json(topic_facts):
    view = get_triple_field_graph(topic_facts)
    return nx.json_graph.node_link_data(view, {'link': 'edges'})


def get_triple_field_tree_json(topic_facts):
    view = get_triple_field_graph(topic_facts)
    reversed_view = view.reverse()
    nodes = list(view.nodes)
    tree_json = []
    for node in nodes:
        treeG = nx.dfs_tree(view, node, depth_limit=1)
        node_json = nx.json_graph.tree_data(treeG, node)
        children = node_json["children"]
        if len(children) > 0:
            node_json["collapsed"] = True
            for child in children:
                child_id = child["id"]
                child["label"] = child_id
                child["id"] = node + "--" + child_id
                child["collapsed"] = True
                reverse_links = reversed_view[child_id]
                reverse_children = []
                for link in reverse_links:
                    reverse_children.append({"id": child_id + "--" + link, "label": link})
                child["children"] = reverse_children

            tree_json.append(node_json)

    return tree_json


def get_relation_field_graph_json(field, topic_facts):
    if field.data_type == 'relation-node':
        return get_node_field_graph_json(topic_facts)
    elif field.data_type == 'relation-triple':
        return get_triple_field_graph_json(topic_facts)


def get_relation_field_tree_json(field, topic_facts):
    if field.data_type == 'relation-node':
        return get_node_field_graph_json(topic_facts)
    elif field.data_type == 'relation-triple':
        tree_json = get_triple_field_tree_json(topic_facts)
        return {"id": field.name, "children": tree_json}


def create_knowledge_graph(workspace_id):
    fields = nosql_db.get_relation_fields_in_workspace(workspace_id)
    G = nx.DiGraph()
    selected_fields = ['86a076d3']
    selected_fields = [field.id for field in fields]
    for field in fields:
        logger.info(f"adding {field.data_type}: {field.name} to knowledge graph.")
        if field.data_type == 'relation-triple' and field.id in selected_fields:
            existing_field_values = nosql_db.read_extracted_field(
                {"field_idx": field.id, "file_idx": "all_files"},
                {"_id": 0, "topic_facts": 1},
            )
            topic_facts = []
            for item in existing_field_values:
                topic_facts.extend(item.get("topic_facts", []))

            for f in topic_facts:
                head_label = f["relation_head"]
                tail_label = f["relation_tail"]
                head_key = normalize_node_text(head_label)
                tail_key = normalize_node_text(tail_label)
                label = field.search_criteria.criterias[0].question
                if head_key and tail_key and head_key != tail_key:
                    if G.has_edge(head_key, tail_key):
                        G.add_edge(head_key, tail_key, size=G[head_key][tail_key]["size"] + 1, field_id=field.id)
                    else:
                        G.add_node(head_key, label=head_label)
                        G.add_node(tail_key, label=tail_label)
                        G.add_edge(head_key, tail_key, size=1, label=label, field_id=field.id)

    def filter_node(n1):
        return nx.degree(G, n1) >= 1

    view = nx.subgraph_view(G, filter_node=filter_node)
    return view


def get_knowledge_tree_json(graph_json, selected_node, depth):
    view = nx.node_link_graph(graph_json)
    tree = view.subgraph(nx.dfs_tree(view, selected_node, depth).nodes())
    return nx.node_link_data(tree, {'link': 'edges'})


def get_knowledge_graph_json(workspace_id):
    view = create_knowledge_graph(workspace_id)
    graph_json = nx.node_link_data(view)
    return graph_json
