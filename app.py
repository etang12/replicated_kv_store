from flask import Flask, jsonify, json, request, Response
from flask.globals import current_app
from requests.api import delete
from requests.exceptions import ConnectionError, Timeout
import requests
import os
import logging
import time

app = Flask(__name__)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

db = {}
down_replicas = {}
# initialize view to grab environment variable "VIEW" which contains default string of replicas
# grab socket address of the container

# logging.info("-----------------START OF CODE-------------------")

view = os.getenv('VIEW').split(',')
thisVC = {}
for v in view:
    thisVC[v] = 0
current_ip = os.environ['SOCKET_ADDRESS']

# logging.info(f"SOCKET ADDRESS: {current_ip}, VIEW: {view}")


TIMEOUT = 3

def delayed(this, other):
    delayMessage = False
    logging.info(f"IN DELAYED: thisVC: {this}, otherVC: {other}")
    for v in view:
        if other[v] > this[v]:
            delayMessage = True
        if other[v] < this[v]:
            delayMessage = False
            break
    return delayMessage

@app.route("/send-db-and-vc/", methods=['GET'])
def send_db_and_vc():
    resp = {}
    resp['db'] = db
    resp['causal-metadata'] = thisVC
    return Response(
        json.dumps(resp), mimetype='application/json'
    )

@app.route("/key-value-store/<key>/<is_forwarded>", methods=['PUT', 'GET', 'DELETE'])
@app.route("/key-value-store/<key>", defaults={'is_forwarded': False}, methods=['PUT', 'GET', 'DELETE'])
def key_value_store(key, is_forwarded):
    # check if in current replica instance

    global current_ip

    # logging.debug(f"\n\nthe current ip: {current_ip}\nis forwarded? {is_forwarded}\n")
    global db
    global thisVC
    global view
    otherVC = request.json['causal-metadata']
    if otherVC == "":
        otherVC = {}
        for v in view:
            otherVC[v] = 0
    logging.info("INSIDE KEY_VALUE_STORE")
    sync_view()
    # logging.debug(f"the type of the recieved vector clock: {type(otherVC)}")
    # logging.debug(f"the recieved vector clock: {otherVC}")
    delayMessage = False 
    # true if otherVC > thisVC (other happens after this)
    # false if otherVC <= thisVC (other is concurrent (causally independant) or happens before this)
    if is_forwarded == False:
        # logging.debug(f"the recieved vector clock: {otherVC}")
        # logging.debug(f"the current vector clock: {thisVC}")
        logging.info(f"before DELAYED: thisVC: {thisVC}, otherVC: {otherVC}")
        delayMessage = delayed(thisVC, otherVC)
        # for v in view:
        #     if otherVC[v] > thisVC[v]:
        #         # thisVC[v] = otherVC[v]
        #         #MIGHT NEED TO CHANGE ABOVE LINE OF CODE GO THROUGH LOGIC AGAIN LATER BUT IM PRETTY SURE ITS CORRECT
        #         delayMessage = True
        #     if otherVC[v] < thisVC[v]:
        #         delayMessage = False
        #         break
    # if is_forwarded == False:
    #     for v in view:
    #         if v != current_ip:
    #             forward_endpoint = f"http://{v}/send-db-and-vc/"
    #             r = requests.get(forward_endpoint)
    #             logging.info("XXXXXXXXXXXXXXXX")
    #             logging.info(r.json())
    #             logging.info(f"type of ['causal-metadata']: {type(r.json()['causal-metadata'])}")
    #             logging.info(r.json()['db'])
    #             logging.info("XXXXXXXXXXXXXXXX")

    if delayMessage:
        for v in view:
            if v != current_ip:
                forward_endpoint = f"http://{v}/send-db-and-vc/"
                r = requests.get(forward_endpoint)
                if delayed(r.json()['causal-metadata'], otherVC) == False:
                    thisVC = r.json()['causal-metadata']
                    db = r.json()['db']
                    delayMessage = False
                    break

   
        # if is_forwarded == False:
        
    if request.method == 'PUT':
        
        put_resp = {}
        if len(key) > 50:
            msg = {"error":"Key is too long","message":"Error in PUT"}
            return Response(
                json.dumps(msg), status=400, mimetype='application/json'
            )
        # if string 'value' exists in the request data
        if 'value' in request.json:
            
            if is_forwarded == False:
                # logging.debug(f"ip address recieving the client: {current_ip}")
                for v in view:
                    if otherVC[v] > thisVC[v]:
                        thisVC[v] = otherVC[v]
                    if v == current_ip:
                        thisVC[v] += 1
                for v in view:
                    if v != current_ip:
                        forward_store(v, key, True, thisVC)
            else:
                # logging.debug(f"ip address recieving from the broadcast replica: {current_ip}")
                thisVC = otherVC
                # state change!

            value = request.json['value']
            if key not in db:
                # logging.debug("adding new key")
                db[key] = value
                put_resp['message'] = "Added successfully"
                put_resp['causal-metadata'] = thisVC
                # put_resp['replaced'] = False
                status = 201
            else:
                # logging.debug("updating old key")
                db[key] = value
                put_resp['message'] = "Updated successfully"
                put_resp['causal-metadata'] = thisVC
                # put_resp['replaced'] = True
                status = 200
                # msg = {"message":"Updated successfully","replaced":True}
        else:
            put_resp['error'] = "Value is missing"
            put_resp['message'] = "Error in PUT"
            status = 400

        logging.info(f"\n\nto client r.content: \n{json.dumps(put_resp)}\n")
        return Response(
            json.dumps(put_resp), status=status, mimetype='application/json'
        )

    elif request.method == 'GET':
        get_resp = {}
        if key in db:
            value = db[key]
            # get_resp['doesExist'] = True
            get_resp['message'] = "Retrieved successfully"
            get_resp['causal-metadata'] = thisVC
            get_resp['value'] = value
            status = 200
        else:
            get_resp['doesExist'] = False
            get_resp['error'] = "Key does not exist"
            get_resp['message'] = "Error in GET"
            status = 404

        logging.info(f"\n\nto client r.content: \n{json.dumps(get_resp)}\n")
        return Response(
            json.dumps(get_resp), status=status, mimetype='application/json'
        )

    elif request.method == 'DELETE':
        del_resp = {}
        if key in db:
            if is_forwarded == False:
                for v in view:
                    if otherVC[v] > thisVC[v]:
                        thisVC[v] = otherVC[v]
                    if v == current_ip:
                        thisVC[v] += 1
                for v in view:
                    if v != current_ip:
                        forward_store(v, key, True, thisVC)
            else:
                thisVC = otherVC
                # state change!

            del db[key]

            # state change!

            # del_resp['doesExist'] = True
            del_resp['message'] = "Deleted successfully"
            del_resp['causal-metadata'] = thisVC
            status = 200
        else:
            del_resp['doesExist'] = False
            del_resp['error'] = "Key does not exist"
            del_resp['message'] = "Error in DELETE"
            status = 404
        
        logging.info(f"\n\nto client r.content: \n{json.dumps(del_resp)}\n")
        return Response(
            json.dumps(del_resp), status=status, mimetype='application/json'
        )

# view operations
@app.route("/key-value-store-view", methods=['GET', 'PUT', 'DELETE'])
def key_value_view():
    #logging.info(view)
    # grab current replica's IP and store in current_ip
    # set the current replica's view to be the default view (grabbed from environment variable), can be found at top of file
    global view
    global current_ip
    # neighbor_replicas = list(view)
    # to_broadcast_put = True
    # to_broadcast_delete = True
    # view = set(view)
    # #logging.info(view)
    # view.remove(current_ip) # remove current_ip (current replica) from view to prevent sending PUT, DELETE requests to itself
    # view = list(view)
    logging.info("------------------IN KEY VALUE FUNCTION------------------")
    logging.info(f"CURRENT REPLICA IP: {current_ip}")
    # time.sleep(2)

    # GET
    if request.method == 'GET':
        # if current_ip not in view:
        #     view.append(current_ip)
        logging.info(view)
        data = {"message":"View retrieved successfully","view":view}
        status = 200
        # for v in view:
        #     forward_view(v)
        return Response(
            json.dumps(data), status=status, mimetype='application/json'
        )

    # PUT
    if request.method == 'PUT':
        data = request.get_json()
        # if data.get('broadcast_flag') is not None:
        #     to_broadcast_put = False
        #logging.info(data)
        replica_added_ip = data.get("socket-address")
        if replica_added_ip in view:
            err_msg = {"error":"Socket address already exists in the view","message":"Error in PUT"}
            status = 404
            return Response(
                json.dumps(err_msg), status=status, mimetype='application/json'
            )
        else:
            view = set(view)
            # logging.info(f"VIEW before PUT in key-value-view: {view}")
            view.add(replica_added_ip)
            view = list(view)
            # logging.info(f"VIEW after PUT in key-value-view: {view}")
            msg = {"message":"Replica added succesfully to the view"}
            status = 201
            # if to_broadcast_put:
            #     # logging.info(f"neighbor_replicas in PUT: {neighbor_replicas}")
            #     # for ip in neighbor_replicas:
            #     for ip in neighbor_replicas:
            #         # time.sleep(1)
            #         logging.info(f"IP IN PUT FOR LOOP:{ip}")
            #         if ip != current_ip:
            #             forward_view(ip, data, neighbor_replicas)
            #view.append(current_ip)
            return Response(
                json.dumps(msg), status=status, mimetype='application/json'
            )

    # DELETE
    if request.method == 'DELETE':
        data = request.get_json()
        # if data.get('broadcast_flag') is not None:
        #     to_broadcast_delete = False
        logging.info(f"\n\n -----------------IN DELETE-------------------")
        logging.info(f"DELETE data: {data}")
        # logging.info(data)
        replica_deleted_ip = data.get("socket-address")
        #logging.info(view)
        # logging.info(replica_deleted_ip)
        if replica_deleted_ip in view:
            view = set(view)
            view.remove(replica_deleted_ip)
            view = list(view)
            # logging.info(view)
            msg = {"message":"Replica deleted successfully from the view"}
            status = 200
            # if to_broadcast_delete:
            #     for ip in neighbor_replicas:
            #         if ip != current_ip:
            #             forward_view(ip, data, neighbor_replicas)
            return Response(
                json.dumps(msg), status=status, mimetype='application/json'
            )
        else:
            msg = {"error":"Socket address does not exist in the view", "message":"Error in DELETE"}
            status = 404
            return Response(
                json.dumps(msg), status=status, mimetype='application/json'
            )

# forwarding function for key-value-store
def forward_store(address, key, is_forwarded, metadata):
    #create the endpoint we wish to forward requests to
    # logging.debug(f"\n\nthe address that we are forwarding to is: {address}\n")
    forward_endpoint = f"http://{address}/key-value-store/{key}/{is_forwarded}"
    #attempt to send requests to that endpoint
    try:
        if request.method == 'PUT':
            data = request.get_json()
            value = data.get('value')
            new_data = {"value": value, "causal-metadata": metadata}
            # logging.debug(f"\n\nforwarding endpoint: {forward_endpoint}\nr: {r}\nr.content: {r.content}\nr.status_code: {r.status_code}\n")

            # logging.debug(f"\n\nforwarding endpoint: {forward_endpoint}\n")

            r = requests.put(forward_endpoint, json=new_data)
            # logging.debug(f"\n\nthe r.url value: {r.url}\n")
        elif request.method == 'GET':
            r = requests.get(forward_endpoint)
            
        elif request.method == 'DELETE':
            new_data = {"causal-metadata": metadata}
            r = requests.delete(forward_endpoint, json=new_data)
    #if attempt to send requests to endpoint fails, then exception is caught and returns error message
    except Exception as e:  
        r_method = request.method
        err_resp = {"error":"Main instance is down", "message":f"Error in {r_method}"}
        return Response(
            json.dumps(err_resp), status=503
        )

    # logging.debug(f"\n\nr.content: {r.content}\nr.status_code: {r.status_code}\n")
    logging.info(f"\n\nto broadcasting replica r.content: \n{r.content}\n")
    return Response(
        r.content, r.status_code
    )

# update view by sending GET requests to all replicas in initial view
# helps ensure new replicas retrieve the most updated view (syncs view for new replicas)
# checks for downed replicas and removes from view
def sync_view():
    global view
    logging.info("\n\n\nINSIDE SYNC_VIEW")
    # updated_view = []
    for replica_ip in view:
        replica_endpoint = f"http://{replica_ip}/key-value-store-view"
        # logging.info(f"REPLICA IP ENDPOINT THAT WE ARE GETTING: {replica_endpoint}")
        if replica_ip != current_ip:
            try:
                response = requests.get(replica_endpoint)
                # logging.info(f"response: {response}, {response.json()}")
                # updated_view = response.json().get('view')
                view = response.json().get('view')
                # logging.info(f"updated view: {view}")
            except Exception as e:
                if replica_ip in view:
                    # logging.info(f"before DELETE view in get_view: {view}")
                    # logging.info(f"replica to delete: {replica_ip}")
                    view = set(view)
                    view.remove(replica_ip)
                    view = list(view)
                    # logging.info(f"after DELETE view in get_view: {view}")
                for v in view:
                    # logging.info(f"VIEW IN FOR LOOP AFTER DELETE: {view}")
                    if current_ip != v:
                        target_replica = f"http://{v}/key-value-store-view"
                        if v != replica_ip:
                            try:
                                r = requests.delete(target_replica, json={"socket-address": replica_ip})
                            except:
                                if v in view:
                                    view = set(view)
                                    view.remove(v)
                                    view = list(view)
    
# broadcast PUT request for new replica to be added to running replica's views
# if request fails for one replica (assume down)
# broadcasts DELETE request with downed replica's ip to all running replicas
def add_replica(ip):
    time.sleep(3)
    global view, down_replicas
    logging.info("\n\nINSIDE ADD REPLICA")
    logging.info(f"VIEW in ADD_REPLICA: {view}")
    logging.info(f"CURRENT_IP: {current_ip}")
        # if replica_ip == current_ip:
        #     view = set(view)
        #     view.add(replica_ip)
        #     view = list(view)
    # send PUT request to all replicas in view
    for replica_ip in view:
        if replica_ip != current_ip:
            replica_endpoint = f"http://{replica_ip}/key-value-store-view"
            logging.info(f"ENDPOINT BEING FORWARDED TO: {replica_endpoint}")
            try:
                r = requests.put(replica_endpoint, json={"socket-address": ip})
            except Exception as e:
                logging.info(f"INSIDE EXCEPTION OF ADD_REPLICA, downed replica: {replica_ip}")
                logging.info(f"ExceptionError: {e}")
                if replica_ip in view:
                    view = set(view)
                    view.remove(replica_ip)
                    view = list(view)
                logging.info(f"AFTER REMOVE IN EXCEPTION: {view}")
                down_replicas = set(down_replicas)
                down_replicas.add(replica_ip)
                for v in view:
                    if current_ip != v:
                        target_replica = f"http://{v}/key-value-store-view"
                        # logging.info(f"REPLICA IP IN BEFORE NESTED EXCEPTION: {replica_ip}")
                        # logging.info(f"FOR LOOP IP IN BEFORE NESTED EXCEPTION: {v}")
                        if v != replica_ip:
                            try:
                                r = requests.delete(target_replica, json={"socket-address": replica_ip})
                            except:
                                if v in view:
                                    down_replicas.add(replica_ip)
                                    # logging.info(f"IN NESTED EXCEPTION")
                                    # logging.info(f"INSIDE NESTED EXCEPTION, downed replica: {v}")
                                    # logging.info(f"INSIDE NESTED EXCEPTION BEFORE DELETE, view: {view}")
                                    view = set(view)
                                    view.remove(v)
                                    # logging.info(f"INSIDE NESTED EXCEPTION AFTER DELETE, view: {view}")
                                    view = list(view)
    
    logging.info("done with adding everything")


if __name__ == "__main__":
    logging.info(f"--------------------IN CONTAINER - CURRENT REPLICA IP: {current_ip}, VIEW: {view}---------------------------")

    add_replica(current_ip)
    
    app.run(host='0.0.0.0', port=8085, debug=False)
