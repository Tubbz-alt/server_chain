import time
from ryu.base import app_manager
from ryu.topology import api as topology_api
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub
from ryu.lib import ofctl_v1_3 as ofctl
from ryu.lib.packet import arp
from ryu.lib.packet import ethernet
from ryu.lib.packet import ipv4
from ryu.lib.packet import packet
from ryu.lib.packet import tcp
from ryu.lib.packet import vlan
from ryu.lib.packet import icmp
from ryu.ofproto import ether
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import ether_types
from ryu.ticomm.etcd import etcd_client
import communal as cm



class ServerChain(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(ServerChain, self).__init__(*args, **kwargs)
        self.etcd_chain = {}  # etcd_client.get_etcd(cm.SERVER_CHAIN)[1]
        self.etcd_node_control = {}  # etcd_client.get_etcd(cm.SERVER_CHAIN_NODE_CONTROL)[1]
        self.etcd_node =  {}  # etcd_client.get_etcd(cm.SERVER_CHAIN_NODE)[1]
        self.etcd_port =  {}  # etcd_client.get_etcd(cm.SERVER_CHAIN_NODE_PORT)[1]
        self.etcd_pool = {}  # etcd_client.get_etcd(cm.SERVER_CHAIN_RESOURCES_POOL)[1]
        self.nwk_mask = None
        self.send_packet = {}
        self.datapaths = {}
        self.thread = hub.spawn(self.detect_node)
        self.datas = cm.DataOperate()
        self.cm = cm.Communal()
        self.reply_params = {}

    # switch and controller link status
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _switch_features_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch(eth_type=ether.ETH_TYPE_ARP, arp_tha=cm.src_mac[0])
        actions = [parser.OFPActionOutput(port=ofproto.OFPP_CONTROLLER,
                                          max_len=ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        # add datapath
        self.logger.info('datapath %s', datapath.id)
        self.datapaths.setdefault(str(datapath.id), datapath)

    # issued flow table
    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    # switch packet to controller
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        self._handle_pkt(msg)

    # get switch interconnection port
    def get_links(self):
        result = {}
        links = topology_api.get_all_link(self)
        if links:
            for s in links:
                dst = s.dst
                if not result.has_key(str(dst.dpid)):
                    result.setdefault(str(dst.dpid), dst.port_no)
                src = s.src
                if not result.has_key(str(src.dpid)):
                    result.setdefault(str(src.dpid), src.port_no)
        return result

    # get proto type
    def get_proto(self, proto_type):
        protos = {'http': 6, 'https': 6, 'tcp': 6, 'udp': 17}
        return protos[proto_type]

    # return correspond list
    def get_data(self, lists, data=None, k=None):
        result = []
        for s in lists:
            if (data and
                    (data.has_key(s) or
                             data.has_key('id') and data['id'] == s)):
                result.append(lists[s])
            elif (k and
                      (lists[s]['node_mode'] is 1 or
                               lists[s].has_key('set_eth') and lists[s]['node_mode'] is 2) and
                          s == k):
                result.append(lists[s])
        return result

    # sMac Forwarding flow
    def flow_action(self, data, action='add', backs='success'):
        # default flood up at table 1
        chain_list = self.get_data(self.etcd_chain, data=data)
        node_list = []
        in_list = []
        out_list = []
        if chain_list:
            for k in data['data']:
                node_arry = self.get_data(self.etcd_node, k=k['node_id'])
                if bool(node_list):
                    node_list.append(node_arry[0])
                else:
                    node_list = node_arry
                for s in self.etcd_port.values():
                    if s['node_id'] == k['node_id']:
                        if node_arry[0]['node_mode'] == 2 and not s.has_key('mac_addres'):
                            return '%s link failure' % s['ip_addres']
                        if not self.datapaths.has_key(s['forwarder']):
                            return 'transponder not exists'
                        if s['interface_type_id'] == 'inside':
                            in_list.append(s)
                        elif s['interface_type_id'] == 'outside':
                            out_list.append(s)
            self.cm.wrapped(self.node_folw,
                            chain=chain_list[0],
                            node_list=node_list,
                            in_list=in_list,
                            out_list=out_list,
                            action=action)

    # set flows param
    def set_flows_param(self, datapath, action, in_port,
                        out_port, src_ip, dst_ip,
                        node_params=[]):
        # # inlet router
        in_node = {}
        if len(node_params):
            in_node = node_params[0]
        self.cm.wrapped(self.flows_params,
                        datapath=datapath[0],
                        action=action,
                        in_port=in_port, out_port=out_port,
                        src_ip=src_ip, dst_ip=dst_ip,
                        node_params=in_node)
        # # outlet router
        out_node = {}
        if len(node_params):
            out_node = node_params[1]
        self.cm.wrapped(self.flows_params,
                        datapath=datapath[1],
                        action=action,
                        in_port=out_port, out_port=in_port,
                        src_ip=dst_ip, dst_ip=src_ip,
                        node_params=out_node)

    # router model
    def router_model(self, i, node_length, in_list, out_list, chain, datas, action):
        dst_ip = chain['dst_ip']
        src_ip = chain['src_ip']
        link_switch = {}
        links = self.get_links()
        if i is 0:
            in_eth = ''
            if in_list[i].has_key('mac_addres'):
                in_eth = in_list[i]['mac_addres']
            arry = self.flow_vlan_action(chain['in_vlan_id'],
                                         in_list[i]['vlan_id'],
                                         in_set_eth=in_eth,
                                         vlan_operate={'out': ''})
            datas.setdefault('datapath', [self.datapaths[in_list[i]['forwarder']],
                                          self.datapaths[chain['in_switch_pdid']]])
            self.set_flows_param(datas['datapath'], action,
                                 chain['in_port'], in_list[i]['port'],
                                 src_ip, dst_ip, arry)
        if node_length == (i+1):
            in_dpid = out_list[i]['forwarder']
            out_dpid = chain['out_switch_pdid']
            in_port = out_list[i]['port']
            out_port = chain['out_port']
            if (in_dpid != out_dpid and
                    links.has_key(in_dpid) and
                    links.has_key(out_dpid)):
                link_switch = links
            datas.setdefault('datapath', [self.datapaths[in_dpid],
                                          self.datapaths[out_dpid]])
            out_eth = ''
            if out_list[i].has_key('mac_addres'):
                out_eth = out_list[i]['mac_addres']
            arry = self.flow_vlan_action(out_list[i]['vlan_id'],
                                         chain['out_vlan_id'],
                                         out_set_eth=out_eth,
                                         vlan_operate={'in': ''})
        else:
            in_dpid = in_list[i]['forwarder']
            out_dpid = out_list[i+1]['forwarder']
            in_port = datas['in'][1]
            out_port = datas['out'][1]
            if (in_dpid != out_dpid and
                    links.has_key(in_dpid) and
                    links.has_key(out_dpid)):
                link_switch = links
            if datas.has_key('datapath'):
                datas.pop('datapath')
            datas.setdefault('datapath', [self.datapaths[in_dpid],
                                          self.datapaths[out_dpid]])
            in_eth = ''
            out_eth = ''
            if out_list[i].has_key('mac_addres'):
                in_eth = in_list[i+1]['mac_addres']
            if in_list[i+1].has_key('mac_addres'):
                out_eth = out_list[i]['mac_addres']
            arry = self.flow_vlan_action(out_list[i]['vlan_id'],
                                         in_list[i+1]['vlan_id'],
                                         in_set_eth=in_eth,
                                         out_set_eth=out_eth,
                                         vlan_operate={'in': '', 'out': ''})
        # is not multiple switch
        if bool(link_switch):
            datapath = [datas['datapath'][0], datas['datapath'][0]]
            self.set_flows_param(datapath, action,
                                 datas['in'][0], link_switch[in_dpid],
                                 src_ip, dst_ip)
            datapath = [datas['datapath'][1], datas['datapath'][1]]
            self.set_flows_param(datapath, action,
                                 link_switch[out_dpid], datas['out'][0],
                                 src_ip, dst_ip)
        else:
            self.set_flows_param(datas['datapath'], action,
                                 in_port, out_port,
                                 src_ip, dst_ip, arry)

    def flow_vlan_action(self, in_vlan, out_vlan,
                         in_set_eth='', out_set_eth='',
                         vlan_operate={}):
        in_params = {}
        out_params = {}
        if in_vlan:
            in_params['dl_vlan'] = in_vlan
            out_params["set_vlan"] = in_vlan
            if in_set_eth:
                in_params["set_eth"] = in_set_eth
        if not in_vlan and vlan_operate.has_key('out'):
            out_params["strip_vlan"] = True
        if out_vlan:
            out_params['dl_vlan'] = out_vlan
            in_params["set_vlan"] = out_vlan
            if out_set_eth:
                out_params["set_eth"] = out_set_eth
        if not out_vlan and vlan_operate.has_key('in'):
            in_params["strip_vlan"] = True
        return [in_params, out_params]

    # Transparent model
    def transparent_model(self, i, node_length, in_list, out_list,
                          chain, datas, action):
        dst_ip = chain['dst_ip']
        src_ip = chain['src_ip']
        link_switch = {}
        links = self.get_links()
        if i is 0:
            arry = self.flow_vlan_action(chain['in_vlan_id'],
                                         in_list[i]['vlan_id'],
                                         vlan_operate={'out': ''})
            datas.setdefault('datapath', [self.datapaths[in_list[i]['forwarder']],
                                          self.datapaths[chain['in_switch_pdid']]])
            self.set_flows_param(datas['datapath'], action,
                                 chain['in_port'], in_list[i]['port'],
                                 src_ip, dst_ip, arry)
        if node_length == (i+1):
            in_port = out_list[i]['port']
            out_port = chain['out_port']
            in_dpid = out_list[i]['forwarder']
            out_dpid = chain['out_switch_pdid']
            if (in_dpid != out_dpid and
                    links.has_key(in_dpid) and
                    links.has_key(out_dpid)):
                link_switch = links
            datas.setdefault('datapath', [self.datapaths[in_dpid],
                                          self.datapaths[out_dpid]])
            arry = self.flow_vlan_action(out_list[i]['vlan_id'],
                                         chain['out_vlan_id'],
                                         vlan_operate={'in': ''})
        else:
            in_dpid = in_list[i]['forwarder']
            out_dpid = out_list[i+1]['forwarder']
            in_port = datas['in'][0]
            out_port = datas['out'][0]
            if (in_dpid != out_dpid and
                    links.has_key(in_dpid) and
                    links.has_key(out_dpid)):
                link_switch = links
            if datas.has_key('datapath'):
                datas.pop('datapath')
            datas.setdefault('datapath', [self.datapaths[in_dpid],
                                          self.datapaths[out_dpid]])
            arry = self.flow_vlan_action(out_list[i]['vlan_id'],
                                         in_list[i + 1]['vlan_id'],
                                         vlan_operate={'in': '', 'out': ''})
        # is not multiple switch
        if bool(link_switch):
            datapath = [datas['datapath'][0], datas['datapath'][0]]
            self.set_flows_param(datapath, action,
                                 datas['in'][0], link_switch[in_dpid],
                                 src_ip, dst_ip)
            datapath = [datas['datapath'][1], datas['datapath'][1]]
            self.set_flows_param(datapath, action,
                                 link_switch[out_dpid], datas['out'][0],
                                 src_ip, dst_ip)
        else:
            self.set_flows_param(datas['datapath'], action,
                                 in_port, out_port,
                                 src_ip, dst_ip, arry)

   

    # get flows params and add flows
    def flows_params(self, datapath, action, in_port, out_port, src_ip, dst_ip,
                     node_params={}):
        params = {"in_port": in_port,
                  "out_port": out_port,
                  "dst_ip": dst_ip,
                  "src_ip": src_ip}
        if node_params.has_key('dl_vlan'):
            params['dl_vlan'] = node_params['dl_vlan']
        if node_params.has_key('strip_vlan'):
            params['strip_vlan'] = node_params['strip_vlan']
        if node_params.has_key('set_eth'):
            params['set_eth'] = node_params['set_eth']
        if node_params.has_key('push_vlan'):
            params['push_vlan'] = node_params['push_vlan']
        if node_params.has_key('set_vlan'):
            params['set_vlan'] = node_params['set_vlan']

        self.cm.wrapped(self.flow_executes,
                        datapath=datapath,
                        params=params, action=action, proto='ip')



    # packet in handle packet
    def _handle_pkt(self, msg):
        datapath = msg.datapath
        port = msg.match['in_port']
        pkt = packet.Packet(data=msg.data)
        pkt_icmp = pkt.get_protocol(icmp.icmp)
        pkt_tcp = pkt.get_protocol(tcp.tcp)
        pkt_arp = pkt.get_protocol(arp.arp)
        ipv4_pkt = pkt.get_protocol(ipv4.ipv4)
        self.logger.info('packet_in %s' % pkt)
        if pkt_arp and pkt_arp.opcode is 2:
            self.reply_params.setdefault(("arp"+pkt_arp.src_ip), pkt_arp.src_mac)
        elif pkt_tcp:
            pkt_tcp = self.reply_tcp_pke(pkt)
            self._send_packet(datapath, pkt_tcp,  port)
            self.reply_params.setdefault(("tcp"+ipv4_pkt.src), ipv4_pkt.src)
        elif pkt_icmp:
            ipv4_pkt = pkt.get_protocol(ipv4.ipv4)
            self.reply_params.setdefault(("icmp"+ipv4_pkt.src), ipv4_pkt.src)
            # re_byte = len(bytes(pkt_icmp.data.data))
            # stime = self.send_packet[ipv4_pkt.src][1]
            # send_byte = self.send_packet[ipv4_pkt.src][0]
            # else:
            #     ofproto = datapath.ofproto
            #     parser = datapath.ofproto_parser
            #     pkt = packet.Packet(msg.data)
            #     eth = pkt.get_protocols(ethernet.ethernet)[0]
            #
            #     if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            #         # ignore lldp packet
            #         return
            #     dst = eth.dst
            #     src = eth.src
            #     dpid = datapath.id
            #     self.mac_to_port.setdefault(dpid, {})
            #     # learn a mac address to avoid FLOOD next time.
            #     self.mac_to_port[dpid][src] = port
            #     if dst in self.mac_to_port[dpid]:
            #         out_port = self.mac_to_port[dpid][dst]
            #     else:
            #         out_port = ofproto.OFPP_FLOOD
            #
            #     actions = [parser.OFPActionOutput(out_port)]
            #     # install a flow to avoid packet_in next time
            #     if out_port != ofproto.OFPP_FLOOD:
            #         match = parser.OFPMatch(in_port=port, eth_dst=dst)
            #         # verify if we have a valid buffer_id, if yes avoid to send both
            #         # flow_mod & packet_out
            #         if msg.buffer_id != ofproto.OFP_NO_BUFFER:
            #             self.add_flow(datapath, 1, match, actions, msg.buffer_id)
            #             return
            #         else:
            #             self.add_flow(datapath, 1, match, actions)
            #     data = None
            #     if msg.buffer_id == ofproto.OFP_NO_BUFFER:
            #         data = msg.data
            #
            #     out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
            #                               in_port=port, actions=actions, data=data)
            #     datapath.send_msg(out)

    # assemble icmp packet
    def assemble_icmp(self, vlanid, dst_ip, dst_mac, src_ip, src_mac, csum, identification):
        offset = ethernet.ethernet._MIN_LEN
        if vlanid != 0:
            ether_proto = ether.ETH_TYPE_8021Q
            vlan_ether = ether.ETH_TYPE_IP
            v = vlan.vlan(0, 0, vlanid, vlan_ether)
            offset += vlan.vlan._MIN_LEN
        else:
            ether_proto = ether.ETH_TYPE_IP
        pkt = packet.Packet()
        if vlanid != 0:
            pkt.add_protocol(v)
        pkt.add_protocol(ethernet.ethernet(ethertype=ether_proto,
                                           dst=dst_mac,
                                           src=src_mac))
        pkt.add_protocol(ipv4.ipv4(dst=dst_ip,
                                   src=src_ip,
                                   csum=csum,
                                   identification=identification,
                                   total_length=60,
                                   ttl=127,
                                   proto=1))
        return pkt

    # tcp shake hands link
    def send_tcp_pke(self, datapath, port, vlan_id, dst_ip, src_ip,
                     src_mac, dst_mac, dport, sport=61300):
        seq = 0
        ack = 0
        offset = 6
        window_size = 8192
        urgent = 0
        option = [tcp.TCPOptionWindowScale(shift_cnt=9),
                  tcp.TCPOptionSACKPermitted(length=2),
                  tcp.TCPOptionTimestamps(ts_val=287454020, ts_ecr=1432778632)]
        if vlan_id != 0:
            ether_proto = ether.ETH_TYPE_8021Q
            vlan_ether = ether.ETH_TYPE_IP
            v = vlan.vlan(0, 0, vlan_id, vlan_ether)
            offset += vlan.vlan._MIN_LEN
        else:
            ether_proto = ether.ETH_TYPE_IP
        pkt = packet.Packet()
        # Add ethernet protocol with ether type IP protocol and mac addresses
        pkt.add_protocol(ethernet.ethernet(ethertype=ether_proto,
                                           dst=dst_mac,
                                           src=src_mac))

        # Add ipv4 protocol with IP addresses and TCP protocol which is 6
        ipv4_pkt = ipv4.ipv4(dst=dst_ip,
                             src=src_ip,
                             proto=6,
                             flags=2)
        pkt.add_protocol(ipv4_pkt)

        # Add tcp protocol with port numbers and sequence number
        tcp_pkt = tcp.tcp(src_port=sport,
                          dst_port=dport,
                          seq=seq,
                          ack=ack,
                          offset=offset,
                          bits=tcp.TCP_SYN,
                          window_size=window_size,
                          urgent=urgent,
                          option=option)
        tcp_pkt.has_flags(tcp.TCP_SYN)
        pkt.add_protocol(tcp_pkt)

        if vlan_id != 0:
            pkt.add_protocol(v)
        self._send_packet(datapath, pkt, port)

    # tcp handshake packet confirm
    def reply_tcp_pke(self, pkt):
        pkt_eth = pkt.get_protocol(ethernet.ethernet)
        pkt_ipv4 = pkt.get_protocol(ipv4.ipv4)
        pkt_tcp = pkt.get_protocol(tcp.tcp)
        pkt = packet.Packet()
        # Add ethernet protocol with ether type IP protocol and mac addresses
        pkt.add_protocol(ethernet.ethernet(ethertype=0x0800,
                                           dst=pkt_eth.dst,
                                           src=pkt_eth.src))

        # Add ipv4 protocol with IP addresses and TCP protocol which is 6
        pkt.add_protocol(ipv4.ipv4(dst=pkt_ipv4.dst,
                                   src=pkt_ipv4.src,
                                   proto=6))

        # Add tcp protocol with port numbers and sequence number
        pkt.add_protocol(tcp.tcp(src_port=pkt_tcp.dst_port,
                                 dst_port=pkt_tcp.src_port,
                                 seq=pkt_tcp.seq + 1,
                                 ack=pkt_tcp.ack,
                                 offset=pkt_tcp.offset,
                                 bits=pkt_tcp.bits,
                                 window_size=pkt_tcp.window_size,
                                 csum=pkt_tcp.csum,
                                 urgent=pkt_tcp.urgent,
                                 option=pkt_tcp.option))
        return pkt

    # get current time
    def get_new_time(self):
        import datetime
        tim = datetime.datetime.now()
        # datetime.datetime.strptime(self.get_now_date(), "%Y-%m-%d %H:%M:%S")
        return tim.strftime("%Y-%m-%d %H:%M:%S")

    # time type transform str type
    def string_toDatetime(self, str_date):
        import datetime
        return datetime.datetime.strptime(str_date, "%Y-%m-%d %H:%M:%S")

    # compare time
    def substract_DateTime(self, dateStr1, dateStr2):
        d1 = self.string_toDatetime(dateStr1)
        d2 = self.string_toDatetime(dateStr2)
        timedelta = d1 - d2
        return (timedelta.days * 24 * 3600 + timedelta.seconds) >= 0

    # detect node whether connect through
    def detect_node(self):
        while True:
            if bool(self.etcd_node_control):
                now_time = str(self.get_new_time())
                for s in self.etcd_node_control:
                    middleware_list = self.etcd_node_control[s]
                    chain = self.etcd_chain[s]
                    for v in middleware_list:
                        node = self.etcd_node[v['node_id']]
                        self.send_several_packet(v, chain, node)
                    data = {'data': middleware_list, 'id': s}
                    if (chain['expired_time'] != '0000-00-00 00:00:00' and
                            self.substract_DateTime(now_time, chain['expired_time'])):
                        self.cm.wrapped(self.flow_action, data=data, action='delete')
            # septum 5m
            time.sleep(10)

    # send several  packet
    def send_several_packet(self, middleware, chain, node):
        type_pkt = chain['detect_type']
        dst_ip = node['manage_ip']
        dst_mac = node['set_eth']
        src_ip = cm.Communal().randomIP(dst_ip)
        src_mac = cm.src_mac[0]
        content = 'abcdefghijklmnopqrstuvwabcdefghi'
        # detect in port or out port
        if type_pkt == 'tcp':
            dport = chain['detect_port']
            if middleware.has_key('in_device_id') and middleware['in_device_id']:
                node_poet = (self.etcd_port[middleware['in_device_id']]
                             if self.etcd_port.has_key(middleware['in_device_id'])
                             else None)
                if node_poet and self.datapaths.has_key(node_poet['forwarder']):
                    datapath = self.datapaths[node_poet['forwarder']]
                    vlanid = node_poet['vlan_id']
                    port = node_poet['port']
                    self.send_tcp_pke(datapath, port, vlanid, dst_ip, src_ip,
                                      src_mac, dst_mac, dport)
                    pkt = self.assemble_tcp(content, vlanid, dst_ip, dst_mac,
                                            src_ip, src_mac, dport)
                self._send_packet(datapath, pkt, port)
            if middleware.has_key('out_device_id') and middleware['out_device_id']:
                node_poet = (self.etcd_port[middleware['out_device_id']]
                             if self.etcd_port.has_key(middleware['out_device_id'])
                             else None)
                if node_poet and self.datapaths.has_key(node_poet['forwarder']):
                    datapath = self.datapaths[node_poet['forwarder']]
                    vlanid = node_poet['vlan_id']
                    port = node_poet['port']
                    self.send_tcp_pke(datapath, port, vlanid, dst_ip, src_ip,
                                      src_mac, dst_mac, dport)
                    pkt = self.assemble_tcp(content, vlanid, dst_ip, dst_mac,
                                            src_ip, src_mac, dport)
                    self._send_packet(datapath, pkt, port)
        elif type_pkt == 'icmp':
            self._send_packet(datapath, pkt, port)
            if middleware.has_key('out_device_id') and middleware['out_device_id']:
                node_poet = (self.etcd_port[middleware['out_device_id']]
                             if self.etcd_port.has_key(middleware['out_device_id'])
                             else None)
                if node_poet and self.datapaths.has_key(node_poet['forwarder']):
                    datapath = self.datapaths[node_poet['forwarder']]
                    vlanid = node_poet['vlan_id']
                    port = node_poet['port']
                    pkt = self.assemble_icmp(vlanid, dst_ip, dst_mac, src_ip,
                                             src_mac, (csum + 2), (identification - 1))
                    pkt.add_protocol(icmp.icmp(type_=icmp.ICMP_ECHO_REQUEST,
                                               code=icmp.ICMP_ECHO_REPLY_CODE,
                                               data=icmp.echo(id_=1, seq=rand + 2, data=content)))
                    self._send_packet(datapath, pkt, port)

    # assemble tcp packet
    def assemble_tcp(self, content, vlanid, dst_ip, dst_mac, src_ip, src_mac, dport, sport=61300):
        offset = ethernet.ethernet._MIN_LEN
        if vlanid != 0:
            ether_proto = ether.ETH_TYPE_8021Q
            vlan_ether = ether.ETH_TYPE_IP
            v = vlan.vlan(0, 0, vlanid, vlan_ether)
            offset += vlan.vlan._MIN_LEN
        else:
            ether_proto = ether.ETH_TYPE_IP
        pkt = packet.Packet()
        if vlanid != 0:
            pkt.add_protocol(v)
        # Add ethernet protocol with ether type IP protocol and mac addresses
        pkt.add_protocol(ethernet.ethernet(ethertype=ether_proto,
                                           dst=dst_mac,
                                           src=src_mac))

        # Add ipv4 protocol with IP addresses and TCP protocol which is 6
        pkt.add_protocol(ipv4.ipv4(dst=dst_ip,
                                   src=src_ip,
                                   proto=6))

        # Add tcp protocol with port numbers and sequence number
        pkt.add_protocol(tcp.tcp(src_port=sport,
                                 dst_port=dport))
        pkt.add_protocol(bytearray(content))
        return pkt

    # assemble arp packet
    def assemble_arp(self, opcode, src_mac, src_ip, dst_ip, vlan_id=0):
        pkt = packet.Packet()
        offset = ethernet.ethernet._MIN_LEN
        if int(vlan_id) != 0:
            ether_proto = ether.ETH_TYPE_8021Q
            vlan_ether = ether.ETH_TYPE_ARP
            v = vlan.vlan(0, 0, int(vlan_id), vlan_ether)
            offset += vlan.vlan._MIN_LEN
        else:
            ether_proto = ether.ETH_TYPE_ARP

        e = ethernet.ethernet('ff:ff:ff:ff:ff:ff', src_mac, ether_proto)
        a = arp.arp(1, 0x0800, 6, 4, opcode, src_mac, src_ip, '00:00:00:00:00:00', dst_ip)
        if int(vlan_id) != 0:
            pkt.add_protocol(v)
        pkt.add_protocol(e)
        pkt.add_protocol(a)
        return pkt

    # send arp
    def thread_arp(self, num, noed_ip, datapath, arp_pkt):
        numbers = num
        while numbers:
            if not self.reply_params.has_key(noed_ip):
                self._send_packet(datapath, arp_pkt)
                numbers -= 1
                time.sleep(1)
            else:
                numbers = 0

    # send packet to switch
    def _send_packet(self, datapath, pkt, port=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        self.logger.info('self.send_packet %s ' % pkt)
        pkt.serialize()
        if not bool(port):
            port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(port=port)]
        out = parser.OFPPacketOut(datapath=datapath,
                                  buffer_id=ofproto.OFP_NO_BUFFER,
                                  in_port=ofproto.OFPP_CONTROLLER,
                                  actions=actions,
                                  data=pkt.data)
        datapath.send_msg(out)






