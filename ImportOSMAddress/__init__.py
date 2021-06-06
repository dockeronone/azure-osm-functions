import logging

import azure.functions as func
from gremlin_python.driver.serializer import GraphSONSerializersV2d0
from gremlin_python.process.graph_traversal import select, __
from gremlin_python.process.traversal import Column
from gremlin_python.structure.graph import GraphTraversalSource
from gremlin_python.driver.client import Client
import osmnx as ox
from shapely.geometry.linestring import LineString
from gremlin_python.process.anonymous_traversal import AnonymousTraversalSource, traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from os import environ
from functools import reduce

def convert_property(value):
    if type(value) is str or type(value) is list:
        return f'\"{value}\"'
    elif type(value) is LineString:
        x, y = value.xy
        return f'\"{list(x)[0]}, {list(x)[1]}, {list(y)[0]}, {list(y)[1]}\"'
    elif type(value) is bool:
        return "true" if value is True else "false"
    else:
        return value

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    gremlin_uri = environ.get("GREMLIN_URI", "")
    gremlin_username = environ.get("GREMLIN_USERNAME", "")
    gremlin_password = environ.get("GREMLIN_PASSWORD", "")

    address = req.params.get('address')
    delete_graph = req.params.get('deleteGraph')

    if not address:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            address = req_body.get('address')
            delete_graph = req_body.get('deleteGraph', False)

    if not delete_graph:
        delete_graph = False

    if address and len(gremlin_uri) > 0:
        nx_g = ox.graph_from_address(address)
        waypoints = list(map(lambda n: {"id": str(n[0]), "waypoint_id": n[0], "address": address, **n[1]}, nx_g.nodes(data=True)))

        client = Client(gremlin_uri,
                        'g',
                        username=gremlin_username, 
                        password=gremlin_password,
                        message_serializer=GraphSONSerializersV2d0())

        g: GraphTraversalSource = traversal().withRemote(DriverRemoteConnection(gremlin_uri, 
                                                                                'g', 
                                                                                username=gremlin_username, 
                                                                                password=gremlin_password,
                                                                                message_serializer=GraphSONSerializersV2d0()))

        # Cleanup
        if delete_graph:
            client.submit("g.V().drop()")

        for waypoint in waypoints:
            query = '.'.join(["g.addV(\"waypoint\")", *list(map(lambda x: f"property(\"{x[0]}\", {convert_property(x[1])})", waypoint.items()))])
            
            try:
                client.submit(query)
            except Exception as e:
                logging.error(f"DB insertion failed with error: {e}")

        for edge in nx_g.edges(data=True):
            v1 = str(edge[0])
            v2 = str(edge[1])

            query = '.'.join([f"g.V(\"{v1}\").addE(\"routes\").to(g.V(\"{v2}\"))", 
                              *list(map(lambda x: f"property(\"{x[0]}\", {convert_property(x[1])})", edge[2].items())),
                              f"property(\"address\", \"{address}\")"])

            try:
                client.submit(query)
            except Exception as e:
                logging.error(f"DB insertion failed with error: {e}")

        client.close()
        return func.HttpResponse(f"This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a address in the query string or in the request body for a personalized response.",
             status_code=200
        )
