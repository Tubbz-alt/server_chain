from ryu.lib import hub
import time


SERVER_CHAIN = '/server_chain/chain_data'
SERVER_CHAIN_NODE = '/server_chain/node_data'
SERVER_CHAIN_NODE_CONTROL = '/server_chain/node_control_data'
SERVER_CHAIN_NODE_PORT = '/server_chain/node_port_data'
SERVER_CHAIN_RESOURCES_POOL = '/server_chain/resource_pool'
stop_time = 2
src_mac = ['52:54:00:56:ae:43']


class DataOperate(object):
    def __init__(self, base_app=None, *args, **kwargs):
        super(DataOperate, self).__init__(*args, **kwargs)
        self.app = base_app
        self.thread = None
        self.check_chain_key = ['chain_name', 'in_switch_pdid', 'in_vlan_id', 'in_port',
                                'out_switch_pdid', 'out_port', 'out_vlan_id']
        self.check_node_control_key = ['node_id', 'in_device_id', 'out_device_id']
        self.check_node_key = ['node_type_id', 'manage_ip']
        self.check_port_key = ['interface_type_id', 'vlan_id', 'port']
        self.check_pool_key = ['pool_name', 'chain_id', 'pool_type', 'chain_port']
        self.cookie = int(''.join(str(ord(n) - 96) for n in 'chain'))

    # add cookie
    def _next_cookie(self, list_type=None):
        self.cookie += 1
        return self.cookie

    # checkdata already exists
    def check_data(self, data, muster_data, check_key):
        msg = ''
        cont = 0
        dic = (muster_data if isinstance(muster_data, list) else muster_data.values())
        for s in dic:
            cont = 0
            # Check the key value
            for k in check_key:
                if isinstance(data, list):
                    num = len(data)
                    while num:
                        if num > 1 and s[k] == data[num-1][k]:
                            cont += 1
                            msg += ('%s already exists !  ' % k)
                        num -= 1
                    return cont, msg
                elif s[k] == data[k]:
                    cont += 1
                    msg += ('%s already exists !  ' % k)
                elif s.has_key('cookie_id'):
                    if self.cookie is int(s["cookie_id"]):
                        self._next_cookie()
        return cont, msg

    # add or update server chain ***********************************************
    def put_chain(self, req):
        status = 400
        data = 'parameter is malformed'
        msg = ''
        cont = 0
        if req:
            dct = eval(req.body)
            muster_data = self.app.etcd_chain
            check_key = self.check_chain_key
            if bool(muster_data):
                cont, msg = Communal().wrapped(self.check_data, data=dct,
                                               muster_data=muster_data,
                                               check_key=check_key)
            if not cont:  # {'id': {111111}}
                if dct.has_key('id'):
                    cid = dct['id']
                    dct.pop('id')
                else:
                    cid = Communal().get_id()
                dct["cookie_id"] = self.cookie
                self.app.etcd_chain.setdefault(cid, dct)
                # etcd_client.update_etcd(SERVER_CHAIN, self.app.etcd_chain)
                status = 200
                data = 'success'
            else:
                status = 400
                if msg:
                    data = msg
        return status, data

    # del server chain
    def del_chain(self, req):
        if req:
            data = eval(req.body)
            if self.app.etcd_chain:
                self.app.etcd_chain.pop(data['id'])
                # etcd_client.update_etcd(SERVER_CHAIN, self.app.etcd_chain)
        return 200, 'success'

    # get server chain
    def get_chain(self):
        return 200, self.app.etcd_chain

    # add or update server chain node control **********************************************
    def put_node_control(self, req):
        status = 400
        data = 'parameter is malformed'
        msg = ''
        cont = 0
        if req:
            dct = eval(req.body)
            check_key = self.check_node_control_key
            if dct:
                muster_data = dct.values()[0]
                if len(muster_data) > 1:
                    cont, msg = Communal().wrapped(self.check_data, data=muster_data,
                                                   muster_data=muster_data,
                                                   check_key=check_key)
            if not cont:  # {'id': {111111}}
                if self.app.etcd_node_control.has_key(dct['id']):
                    action = 'modify'
                else:
                    action = 'add'
                self.app.etcd_node_control.setdefault(dct['id'], dct['data'])
                # etcd_client.update_etcd(SERVER_CHAIN_NODE_CONTROL, self.app.etcd_node_control)
                data = Communal().wrapped(self.app.flow_action, data=dct, action=action)
                status = 200
            else:
                status = 400
                if msg:
                    data = msg
        return status, data

    # del server chain node control
    def del_node_control(self, req):
        result = 'params error'
        if req:
            data = eval(req.body)
            if self.app.etcd_node_control:
                dic = self.app.etcd_node_control[data['id']]
                self.app.etcd_node_control.pop(data['id'])
                # etcd_client.update_etcd(SERVER_CHAIN_NODE_CONTROL, self.app.etcd_node_control)
                result = Communal().wrapped(self.app.flow_action, data=dic, action='delete')
        return 200, result

    # get server chain node control
    def get_node_control(self):
        return 200, self.app.etcd_node_control

    # add or update server chain node **********************************************
    def put_node(self, req):
        status = 400
        data = 'parameter is malformed'
        msg = ''
        cont = 0
        if req:
            dct = eval(req.body)
            muster_data = self.app.etcd_node
            check_key = self.check_node_key
            if bool(muster_data):
                cont, msg = Communal().wrapped(self.check_data, data=dct,
                                               muster_data=muster_data,
                                               check_key=check_key)
            if not cont:  # {'id': {111111}}
                if dct.has_key('id'):
                    cid = dct['id']
                    dct.pop('id')
                else:
                    cid = Communal().get_id()
                self.app.etcd_node.setdefault(cid, dct)
                # etcd_client.update_etcd(SERVER_CHAIN_NODE, self.app.etcd_node)
                status = 200
                data = 'success'
            else:
                status = 400
                if msg:
                    data = msg
        return status, data

    # del server chain node
    def del_node(self, req):
        if req:
            data = eval(req.body)
            if self.app.etcd_node_control:
                self.app.etcd_node.pop(data['id'])
                # etcd_client.update_etcd(SERVER_CHAIN_NODE, self.app.etcd_node)
        return 200, 'success'

    # get server chain node
    def get_node(self):
        return 200, self.app.etcd_node

    # add or update server node port ***********************************************
    def put_node_port(self, req):
        status = 400
        data = 'parameter is malformed'
        msg = ''
        cont = 0
        if req:
            dct = eval(req.body)
            muster_data = self.app.etcd_port
            check_key = self.check_port_key
            if bool(muster_data):
                cont, msg = Communal().wrapped(self.check_data, data=dct,
                                               muster_data=muster_data, check_key=check_key)
            # send arp packet
            if dct.has_key('ip_addres'):
                port_ip = dct['ip_addres']
                src_ip = Communal().randomIP(port_ip)
                arp_pkt = self.app.assemble_arp(1, src_mac[0], src_ip,
                                                dct['manage_ip'], dct['vlan_id'])
                if self.app.datapaths.has_key(dct['forwarder']):
                    datapath = self.app.datapaths[dct['forwarder']]
                    self.thread = hub.spawn_after(stop_time, self.app.thread_arp,
                                                  num=3, noed_ip=("arp"+port_ip),
                                                  datapath=datapath, arp_pkt=arp_pkt)
                    time.sleep(2)
                    if self.app.reply_params.has_key("arp"+port_ip):
                        dct['mac_addres'] = self.app.reply_params["arp"+port_ip]
                        self.app.reply_params.pop("arp"+port_ip)
            else:
                node_list = self.app.etcd_node[dct['node_id']]
                noed_ip = node_list['manage_ip']
                src_ip = Communal().randomIP(node_list['manage_ip'])
                arp_pkt = self.app.assemble_arp(1, src_mac[0], src_ip, noed_ip)
                if self.app.datapaths.has_key(dct['forwarder']):
                    datapath = self.app.datapaths[dct['forwarder']]
                    self.thread = hub.spawn_after(stop_time, self.app.thread_arp,
                                                  num=3, noed_ip=("arp"+noed_ip),
                                                  datapath=datapath, arp_pkt=arp_pkt)
                    time.sleep(1)
                    if self.app.reply_params.has_key("arp"+noed_ip):
                        self.app.etcd_node[dct['node_id']]['set_eth'] = self.app.reply_params["arp" + noed_ip]
                        self.app.reply_params.pop("arp" + noed_ip)
            if (dct.has_key('ip_addres') and
                    not dct.has_key('mac_addres') or not node_list.has_key('set_eth')):
                status = 400
                data = 'the connection is not accessible'
            elif not cont:
                if dct.has_key('id'):
                    cid = dct['id']
                    dct.pop('id')
                else:
                    cid = Communal().get_id()
                self.app.etcd_port.setdefault(cid, dct)
                # etcd_client.update_etcd(SERVER_CHAIN_NODE_PORT, self.app.etcd_port)
                status = 200
                data = 'success'
            else:
                status = 400
                if msg:
                    data = msg
        return status, data

    # del server node port
    def del_node_port(self, req):
        if req:
            data = eval(req.body)
            if self.app.etcd_port:
                self.app.etcd_port.pop(data['id'])
                # etcd_client.update_etcd(SERVER_CHAIN_NODE_PORT, self.app.etcd_port)
        return 200, 'success'

    # get server node port
    def get_node_port(self):
        return 200, self.app.etcd_port

    # add or update server node port ***********************************************
    def put_pool(self, req):
        status = 400
        data = 'parameter is malformed'
        msg = ''
        cont = 0
        if req:
            dct = eval(req.body)
            muster_data = self.app.etcd_pool
            check_key = self.check_port_key
            if bool(muster_data):
                cont, msg = Communal().wrapped(self.check_data, data=dct,
                                               muster_data=muster_data,
                                               check_key=check_key)
            if cont and cont > 1:  # {'id': {111111}}
                cid = Communal().get_id()
                self.app.etcd_pool.setdefault(cid, dct)
                # etcd_client.update_etcd(SERVER_CHAIN_NODE_PORT, self.app.etcd_port)
                status = 200
                data = 'success'
            else:
                status = 400
                if msg:
                    data = msg
        return status, data

    # del server node port
    def del_pool(self, req):
        if req:
            data = eval(req.body)
            if self.app.etcd_pool:
                self.app.etcd_pool.pop(data['id'])
                # etcd_client.update_etcd(SERVER_CHAIN_RESOURCES_POOL, self.app.etcd_pool)
        return 200, 'success'

    # get server node port
    def get_pool(self):
        return 200, self.app.etcd_pool

    # get server node port ***********************************************
    def get_info(self):
        return 200, self.app.etcd_chain

    # get server node path
    def get_info_path(self):
        return 200, self.app.etcd_chain

    # get server node flowfeature
    def get_info_flowfeature(self):
        return 200, self.app.etcd_chain


class Communal(object):
    # General check
    def wrapped(self, func, req=None, **kwargs):
        try:
            if req:
                return func(req, **kwargs)
            else:
                return func(**kwargs)
        except Exception, e:
            return 'error', e

    # Generates a random id, create only id
    def get_id(self):
        import random
        import time
        ints = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        chars = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
                 'U', 'V', 'W', 'X', 'Y', 'Z',
                 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
                 'u', 'v', 'w', 'x', 'y', 'z']

        rd_char1 = random.randint(5, 6)
        rd_int2 = random.randint(5, 6)
        rd_char2 = random.randint(5, 6)
        slices = [str(int(time.time().conjugate()))]
        slices.extend(random.sample(chars, rd_char1))
        slices.extend(random.sample(ints, rd_int2))
        slices.extend(random.sample(chars, rd_char2))
        # timestamp = int(slices)
        # time.strftime('%Y-%m-%d %X', time.localtime(timestamp))
        return ''.join(slices)

    # get random mac
    def randomMAC(self):
        import random
        mac = [0x52, 0x54, 0x00,
               random.randint(0x00, 0x7f),
               random.randint(0x00, 0xff),
               random.randint(0x00, 0xff)]
        return ':'.join(map(lambda x: "%02x" % x, mac))

    # get random mac
    def randomIP(self, dst_ip):
        match = str(dst_ip).split('.')
        num = match[3]
        ip_address = ''
        for s in range(2, 254):
            val = str(s)
            if val != num:
                match.pop(3)
                match.insert(3, val)
                ip_address = '.'.join(match)
                break
        return ip_address