from rdflib import Graph, URIRef, Literal, Namespace

def convert_to_rdf(row):
    ns = Namespace("http://qlinx.oneqic.co.kr/orders/")
    g = Graph()
    order_uri = URIRef(ns + f"order_{row['order_id']}")
    
    g.add((order_uri, ns.departure, Literal(row["departure"])))
    g.add((order_uri, ns.destination, Literal(row["destination"])))
    g.add((order_uri, ns.customer, Literal(row["customer"])))
    g.add((order_uri, ns.status, Literal(row["status"])))
    
    return g.serialize(format="turtle")