#ifndef RPCS_H
#define RPCS_H

#include "chord.h"
#include "rpc/client.h"
#include "string.h"
#include "math.h"

#include <iostream>
#include <vector>

#define FINGER_SIZE 8
#define M_BIT 32
#define SUCCESSOR_SIZE 3

Node self, predecessor;
std::vector<Node> successor(SUCCESSOR_SIZE);
Node finger[FINGER_SIZE];
int16_t next;  // next idx in finger table
bool in_ring = false;

bool is_between(uint64_t target, uint64_t n1, uint64_t n2, bool r_bound = false) {
  if (n2 > n1) {
    if (target > n1 && target < n2)
      return true;
    if (r_bound && target > n1 && target <= n2)
      return true;
  } else if (n1 > n2) {
    if (target < n2 || target > n1)
      return true;
    if (r_bound && (target <= n2 || target > n1))
      return true;
  } else { 
    return true;
  }
  return false;
}

Node get_info() { return self; } // Do not modify this line.

Node get_predecessor() {
  if (predecessor.ip == "")
    return {"", 0, 0};
  return predecessor;
}

std::vector<Node> get_successor_list() {
  return successor;
}

void create() {
  predecessor.ip = "";
  successor.assign(SUCCESSOR_SIZE, self);
  next = 0;
  in_ring = true;
  for (int i = 0; i < FINGER_SIZE; i++)
    finger[i] = {self.ip, self.port, self.id};
  // std::cout << "created, self: " << self.id << "\n";
}

void join(Node n) {
  predecessor.ip = "";
  rpc::client client(n.ip, n.port);
  Node new_succ = client.call("find_successor", self.id).as<Node>();  // check timeout?
  // get new_succ's sucessor list
  std::vector<Node> new_s_list;
  bool update_s = true;
  if (new_succ.id == self.id) {
    update_s = false;
  } else if (new_succ.id == n.id) {
    new_s_list = client.call("get_successor_list").as<std::vector<Node>>();  // check timeout?
    client.call("notify", self);
  } else {
    rpc::client cli_new_succ(new_succ.ip, new_succ.port);
    new_s_list = cli_new_succ.call("get_successor_list").as<std::vector<Node>>();  // check timeout?
    cli_new_succ.call("notify", self);
  }
  if (update_s) {
    successor[0] = new_succ;
    for (int i = 1; i < SUCCESSOR_SIZE; i++)
      successor[i] = new_s_list[i-1];
  }
  next = 0;
  in_ring = true;
  for (int i = 0; i < FINGER_SIZE; i++)
    finger[i] = {self.ip, self.port, self.id};
  // std::cout << "joined, self: " << self.id << ", successor: " << successor[0].id << "\n";
}

Node closest_preceding_node(uint64_t id) {
  for (int i = FINGER_SIZE - 1; i >= 0; i--){
    if (is_between(finger[i].id, self.id, id))  {
      // check liveness before returning finger
      try {
        // std::cout << self.port << " find " << id << ", next node: " << finger[i].port << "\n";
        rpc::client chk_finger(finger[i].ip, finger[i].port);
        chk_finger.call("get_info").as<Node>();
        // std::cout << "return finger\n";
        return finger[i];
      } catch (std::exception &e) {
        // std::cout << "closest preceding finger error\n";
        continue;
      }
    }
  }
  /*
  std::cout << self.port << " cannot find closest preceding node\n";
  std::cout << "self.id: " << self.id << "; target_id: " << id << "\n";
  for (int i = FINGER_SIZE - 1; i >= 0; i--)
    std::cout << finger[i].id << " ";
  std::cout << "\n";
  */
  return self;
}

Node find_successor(uint64_t id) {
  // std::cout << "find successor\n";
  // std::cout << id << " " << self.id << " " << successor.id << "\n";

  for (int i = 0; i < SUCCESSOR_SIZE; i++) {
    // check if the successor is alive
    /*
    try {
      if (successor[i].id != self.id){
        rpc::client chk_succ(successor[i].ip, successor[i].port);
        Node tmp = chk_succ.call("get_info").as<Node>();
      }
    } catch (std::exception &e) {
      // if (self.port == 5060)
      //   std::cout << "find_successor catch\n";
      continue;
    }
    */

    if (is_between(id, self.id, successor[i].id, true))
      return successor[i];
    else if (is_between(id, predecessor.id, self.id, true))
      return self;
    else {
      Node next_node = closest_preceding_node(id);
      if (next_node.id == self.id)
        return successor[i];
      rpc::client cli_next(next_node.ip, next_node.port);  // check timeout?
      // std::vector<Node> next_s_list = cli_next.call("get_successor_list").as<std::vector<Node>>();
      /*
      if (next_s_list[0].id == self.id) {
        if (is_between(id, next_node.id, self.id, true))
          return self;
      }
      */
      return cli_next.call("find_successor", id).as<Node>();
    }
  }
  // if (self.port == 5062)
  //   std::cout << "\n";
  return self;
}

void notify(Node n) {
  if (predecessor.ip == "") {
    predecessor = n;
    return;
  }
  if (is_between(n.id, predecessor.id, self.id))
    predecessor = n;
}

void stabilize() {
  // check_predecessor();
  // x = successor.predecessor;
  if (!in_ring)
    return;

  Node x = {"", 0, 0};
  std::vector<Node> s_list, ns_list;
  for (int i = 0; i < SUCCESSOR_SIZE; i++) {
    try {
      // get successor's predecessor and succ_list
      if (successor[i].id == self.id) {
        x = predecessor;
        s_list = successor;
      } else {
        rpc::client succ(successor[i].ip, successor[i].port);
        x = succ.call("get_predecessor").as<Node>();
        s_list = succ.call("get_successor_list").as<std::vector<Node>>();
        // notify here?
        succ.call("notify", self);
      }
      // update succ_list
      successor[0] = successor[i];
      for (int i = 1; i < SUCCESSOR_SIZE; i++) 
        successor[i] = s_list[i - 1];

      // update successor[i]
      if (x.ip != "" && is_between(x.id, self.id, successor[i].id)) {
        try {
          rpc::client new_succ(x.ip, x.port);
          ns_list = new_succ.call("get_successor_list").as<std::vector<Node>>();
          successor[0] = x;
          for (int i = 1; i < SUCCESSOR_SIZE; i++)
            successor[i] = ns_list[i - 1];
          // notify pos1
          new_succ.call("notify", self);
        } catch (std::exception &e) {
          // std::cout << self.port << ": stabilize in loop error\n";
        }
      }
      break;
      
    } catch (std::exception &e) {
      // std::cout << "stabilize catch " << successor[i].port << " " << successor[i].id << " died\n";
      continue;
    }
  }
}

// finger table stores n+2^28 ~ n+2^31, table idx starts at 0
void fix_fingers() {  
  if (!in_ring)
    finger[next] = successor[0];
  else{
    // std::cout << "fix " << self.port << " idx: " << next << (self.id + uint64_t(pow(2, M_BIT - FINGER_SIZE + next))) % uint64_t(pow(2, M_BIT)) << "\n";
    try {
      finger[next] = find_successor((self.id + uint64_t(pow(2, M_BIT - FINGER_SIZE + next))) % uint64_t(pow(2, M_BIT)));
    } catch (std::exception &e) {
      // std::cout << self.port << " fix finger error\n";
    }
  }
  next = next + 1;
  if (next > FINGER_SIZE - 1)
    next = 0;
}

void check_predecessor() {
  try {
    rpc::client chk_pred(predecessor.ip, predecessor.port);
    Node n = chk_pred.call("get_info").as<Node>();
  } catch (std::exception &e) {
    // printf("check_predecessor error\n");
    predecessor.ip = "";
  }
}

void register_rpcs() {
  add_rpc("get_info", &get_info); // Do not modify this line.
  add_rpc("create", &create);
  add_rpc("join", &join);
  add_rpc("find_successor", &find_successor);
  add_rpc("closest_preceding_node", &closest_preceding_node);
  add_rpc("notify", &notify);
  add_rpc("get_predecessor", &get_predecessor);
  add_rpc("get_successor_list", &get_successor_list);
}

void register_periodics() {
  add_periodic(stabilize);
  add_periodic(fix_fingers);
  add_periodic(check_predecessor);
}

#endif /* RPCS_H */
