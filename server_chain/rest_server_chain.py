# -*- coding: utf-8 -*-
import json
import logging
from webob import Response
from ryu.app.wsgi import ControllerBase, WSGIApplication, route
import server_chain
import communal as cm

chain = '/chain'
node = '/chain_node'
control = '/node_control'
node_port = '/node_port'
info = '/chain_info'


class RestServerChain(server_chain.ServerChain):
    _CONTEXTS = {'wsgi': WSGIApplication}

    def __init__(self, *args, **kwargs):
        super(RestServerChain, self).__init__(*args, **kwargs)
        wsgi = kwargs['wsgi']
        wsgi.register(ChainController, {'ServerChain': self})


class ChainController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(ChainController, self).__init__(req, link, data, **config)
        self.app = data['ServerChain']
        self.data = cm.DataOperate(self.app)

    # api response
    def rest_response(self, func, req=None):
        try:
            msg, data = self.app.cm.wrapped(func, req)
            status = [500 if isinstance(msg, str) else msg][0]
            body = json.dumps({'data': data})
        except Exception as e:
            body = json.dumps({'data': str(e)})
        return Response(status=status, content_type='application/json', body=body)

    # add update delete get chain ***********************************************
    @route('chain', chain, methods=['GET', 'POST'])
    def put_chain(self, req, **kwargs):
        data = {}
        if req and req.body:
            data = eval(req.body)
        if req.method == 'POST' and data:
            if data.has_key('id') and len(data) == 1:  # delete
                return self.rest_response(self.data_app.del_chain, data)
            else:  # add or update
                return self.rest_response(self.data_app.put_chain, data)
        elif req.method == 'GET':  # get
            return self.rest_response(self.data_app.get_chain)

    # add update delete get chain node control**********************************************
    @route('control', control, methods=['GET', 'POST'])
    def put_node_control(self, req, **kwargs):
        data = {}
        if req and req.body:
            data = eval(req.body)
        if req.method == 'POST' and data:
            if data.has_key('id') and len(data) == 1:  # delete
                return self.rest_response(self.data_app.del_node_control, data)
            else:  # add or update
                return self.rest_response(self.data_app.put_node_control, data)
        elif req.method == 'GET':  # get
            return self.rest_response(self.data_app.get_node_control)

    # add or update server chain node **********************************************
    @route('node', node, methods=['GET', 'POST'])
    def put_node(self, req, **kwargs):
        data = {}
        if req and req.body:
            data = eval(req.body)
        if req.method == 'POST' and data:
            if data.has_key('id') and len(data) == 1:  # delete
                return self.rest_response(self.data_app.del_node, data)
            else:  # add or update
                return self.rest_response(self.data_app.put_node, data)
        elif req.method == 'GET':  # get
            return self.rest_response(self.data_app.get_node)

    # add or update server chain node port ***********************************************
    @route('node_port', node_port, methods=['GET', 'POST'])
    def put_node_port(self, req, **kwargs):
        data = {}
        if req and req.body:
            data = eval(req.body)
        if req.method == 'POST' and data:
            if data.has_key('id') and len(data) == 1:  # delete
                return self.rest_response(self.data_app.del_node_port, data)
            else:  # add or update
                return self.rest_response(self.data_app.put_node_port, data)
        elif req.method == 'GET':  # get
            return self.rest_response(self.data_app.get_node_port)

    # add or update server chain node port ***********************************************
    # @route('pool', pool+'put', methods=['POST'])
    # def put_pool(self, req, **kwargs):
    #     return self.rest_response(self.data.put_pool, req)
    #
    # # delete server chain node
    # @route('pool', pool+'del', methods=['POST'])
    # def del_pool(self, req, **kwargs):
    #     return self.rest_response(self.data.del_pool, req)
    #
    # # select Load balancer node
    # @route('pool', pool+'get', methods=['GET'])
    # def get_pool(self, req, **kwargs):
    #     return self.rest_response(self.data.get_pool)

    # get server chain info ***********************************************
    @route('info', info+'get', methods=['POST'])
    def get_info(self, req, **kwargs):
        return self.rest_response(self.data.get_info)

    # get server node path
    @route('info', info+'get_path', methods=['POST'])
    def get_info(self, req, **kwargs):
        return self.rest_response(self.data.get_info_path)

    # get server node flowfeature
    @route('info', info+'get_flowfeature', methods=['POST'])
    def get_info(self, req, **kwargs):
        return self.rest_response(self.data.get_info_flowfeature)


