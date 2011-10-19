open Printf
open Unix
open FSTypes2

type connection
external castle_connect : unit -> connection = "caml_castle_connect"
external castle_disconnect : connection -> unit = "caml_castle_disconnect"

external castle_device_to_devno : string -> int32 = "caml_castle_device_to_devno"
external castle_devno_to_device : int32 -> string = "caml_castle_devno_to_device"

external castle_fd : connection -> file_descr = "caml_castle_fd"

(* Data path *)
external castle_get : connection -> int32 -> string array -> string = "caml_castle_get"
external castle_replace : connection -> int32 -> string array -> string -> unit = "caml_castle_replace"
external castle_remove : connection -> int32 -> string array -> unit = "caml_castle_remove"
external castle_iter_start : connection -> int32 -> string array -> string array -> int -> int32 * bool * ((string array * string) array) = "caml_castle_iter_start"
external castle_iter_next : connection -> int32 -> int -> bool * ((string array * string) array) = "caml_castle_iter_next"
external castle_iter_finish : connection -> int32 -> unit = "caml_castle_iter_finish"
external castle_get_slice : connection -> int32 -> string array -> string array -> int -> (string array * string) array = "caml_castle_get_slice"

(* Control Path *)
external castle_claim                           : connection -> int32 -> int32 = "caml_castle_claim"
external castle_attach                          : connection -> int32 -> int32 = "caml_castle_attach"
external castle_detach                          : connection -> int32 -> unit = "caml_castle_detach"
external castle_snapshot                        : connection -> int32 -> int32 = "caml_castle_snapshot"
external castle_collection_attach               : connection -> int32 -> string -> int -> int32 = "caml_castle_collection_attach"
external castle_collection_reattach             : connection -> int32 -> int32 -> unit = "caml_castle_collection_reattach"
external castle_collection_detach               : connection -> int32 -> unit = "caml_castle_collection_detach"
external castle_collection_snapshot             : connection -> int32 -> int32 = "caml_castle_collection_snapshot"
external castle_create                          : connection -> int64 -> int32 = "caml_castle_create"
external castle_delete_version                  : connection -> int32 -> unit = "caml_castle_delete_version"
external castle_destroy_vertree                 : connection -> int32 -> unit = "caml_castle_destroy_vertree"
external castle_vertree_compact                 : connection -> int32 -> unit = "caml_castle_vertree_compact"
external castle_clone                           : connection -> int32 -> int32 = "caml_castle_clone"
external castle_init                            : connection -> unit = "caml_castle_init"
external castle_fault                           : connection -> int32 -> int32 -> unit = "caml_castle_fault"
external castle_environment_set                 : connection -> int32 -> string -> unit = "caml_castle_environment_set"
external castle_slave_evacuate                  : connection -> int32 -> int32 -> unit = "caml_castle_slave_evacuate"
external castle_slave_scan                      : connection -> int32 -> unit = "caml_castle_slave_scan"
external castle_thread_priority                 : connection -> int32 -> unit = "caml_castle_thread_priority"
external castle_ctrl_prog_deregister            : connection -> bool -> int32 = "caml_castle_ctrl_prog_deregister"

let connect _ _ _ =
        castle_connect ()

let disconnect connection = 
        castle_disconnect connection

let connection_fd conn = castle_fd conn

exception Invalid_reply of string
exception Invalid_iterator

let _ = Callback.register_exception "acunuClientLib2 Not_found" Not_found

let lib_init () = ()

(* Data Path *)

let get conn c k = 
        try Value (castle_get conn c k)
        with Not_found -> Tombstone 

let remove conn c k = castle_remove conn c k

let replace conn c k v = castle_replace conn c k v

let iter_start connection c start finish batch_size = 
	let token, more, arr = castle_iter_start connection c start finish batch_size in
		(token, more, Array.map (fun (k,v) -> (k, Value v)) arr)
let iter_next connection t batch_size = 
	let more, arr = castle_iter_next connection t batch_size in
		(more, Array.map (fun (k,v) -> (k, Value v)) arr)
let iter_finish connection t = castle_iter_finish connection t
(* 'limit' means the maximum number of values to return. 0 means unlimited. *)
let get_slice connection c start finish limit = Array.map (fun (k,v) -> (k, Value v)) (castle_get_slice connection c start finish limit)

(*****************************************
 * Things not implemented by new interface 
 *****************************************)

let nimsg = "Not implemented in new interface but will be Soon (HW)"
let multi_replace connection c kvps = failwith nimsg
let iter_replace_last connection t i v = failwith nimsg

(* Control Path *)

let claim connection ~device = castle_claim connection device
let claim_dev connection ~device = castle_claim connection (castle_device_to_devno device)

(* get a device id for the given version. *)
let attach connection ~version = castle_attach connection version
let attach_dev connection ~version = castle_devno_to_device(attach connection ~version)

let detach connection ~(device:int32) = castle_detach connection device
let detach_dev connection ~(device:string) = castle_detach connection (castle_device_to_devno device)

let create connection ~size = castle_create connection size

let destroy_vertree connection ~vertree = castle_destroy_vertree connection vertree
let vertree_compact connection ~vertree = castle_vertree_compact connection vertree
let delete_version connection ~version = castle_delete_version connection version
let fault   connection ~(fault_id:int32) ~(fault_arg :int32)= castle_fault connection fault_id fault_arg

let slave_evacuate connection ~(disk:int32) ~(force:int32) = castle_slave_evacuate connection disk force
let slave_scan    connection ~id = castle_slave_scan connection id
let thread_priority  connection ~nice_value = castle_thread_priority connection nice_value
let ctrl_prog_deregister connection ~shutdown = castle_ctrl_prog_deregister connection shutdown

(* Create a child version of the given one, and return the new version id. *)
let clone connection ~(version:int32) = castle_clone connection version

(* Create a child version of the one mapped to by this device id (to get a *)
(* device id for a version, use 'attach'). Returns the version id of the   *)
(* new leaf (and updates the device id to point to the new leaf).          *)
let snapshot connection ~(device: int32) = castle_snapshot connection device
let snapshot_dev connection ~(device: string) = castle_snapshot connection (castle_device_to_devno device)

let init connection = castle_init connection

let collection_attach connection ~(version:int32) ~name = 
        castle_collection_attach connection version name (String.length name)

let collection_reattach connection ~(collection:int32) ~(new_version:int32) =
        castle_collection_reattach connection collection new_version

let collection_detach connection ~(collection:int32) = 
        castle_collection_detach connection collection

let collection_take_snapshot connection ~(collection:int32) = 
        castle_collection_snapshot connection collection

let environment_set connection id data =
  let id_n = match id with
    BuildId -> 0l
  | ModuleHash -> 1l
  | Description -> 2l
  | Hostname -> 3l
  in
  castle_environment_set connection id_n data
