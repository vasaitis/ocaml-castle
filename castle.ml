open Printf
open Unix
open FSTypes2

type connection
external castle_connect : unit -> connection = "caml_castle_connect"
external castle_disconnect : connection -> unit = "caml_castle_disconnect"

external castle_device_to_devno : string -> int32 = "caml_castle_device_to_devno"
external castle_devno_to_device : int32 -> string = "caml_castle_devno_to_device"

(* Data path *)
external castle_get : connection -> int32 -> string array -> string = "caml_castle_get"
external castle_replace : connection -> int32 -> string array -> string -> unit = "caml_castle_replace"
external castle_remove : connection -> int32 -> string array -> unit = "caml_castle_remove"
external castle_iter_start : connection -> int32 -> string array -> string array -> int32 = "caml_castle_iter_start"
external castle_iter_next : connection -> int32 -> int -> (string array * string) array = "caml_castle_iter_next"
external castle_iter_finish : connection -> int32 -> unit = "caml_castle_iter_finish"
external castle_get_slice : connection -> int32 -> string array -> string array -> int -> (string array * string) array = "caml_castle_get_slice"

(* Control Path *)
external castle_claim                           : connection -> int32 -> int32 = "caml_castle_claim"
external castle_release                         : connection -> int32 -> unit = "caml_castle_release"
external castle_attach                          : connection -> int32 -> int32 = "caml_castle_attach"
external castle_detach                          : connection -> int32 -> unit = "caml_castle_detach"
external castle_snapshot                        : connection -> int32 -> int32 = "caml_castle_snapshot"
external castle_collection_attach               : connection -> int32 -> string -> int -> int32 = "caml_castle_collection_attach"
external castle_collection_detach               : connection -> int32 -> unit = "caml_castle_collection_detach"
external castle_collection_snapshot             : connection -> int32 -> int32 = "caml_castle_collection_snapshot"
external castle_create                          : connection -> int64 -> int32 = "caml_castle_create"
external castle_destroy                         : connection -> int32 -> int32 -> unit = "caml_castle_destroy"
external castle_clone                           : connection -> int32 -> int32 = "caml_castle_clone"
external castle_init                            : connection -> unit = "caml_castle_init"
external castle_fault                           : connection -> int32 -> int32 -> unit = "caml_castle_fault"
external castle_environment_set                 : connection -> int32 -> string -> unit = "caml_castle_environment_set"
external castle_slave_evacuate                  : connection -> int32 -> int32 -> unit = "caml_castle_slave_evacuate"
external castle_slave_scan                      : connection -> int32 -> unit = "caml_castle_slave_scan"
external castle_thread_priority                 : connection -> int32 -> unit = "caml_castle_thread_priority"

let connect _ _ _ =
        castle_connect ()

let disconnect connection = 
        castle_disconnect connection

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

let iter_start connection c start finish = castle_iter_start connection c start finish
let iter_next connection t batch_size = Array.map (fun (k,v) -> (k, Value v)) (castle_iter_next connection t batch_size)
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

let release connection ~(disk:int32) = castle_release connection disk

(* get a device id for the given version. *)
let attach connection ~version = castle_attach connection version
let attach_dev connection ~version = castle_devno_to_device(attach connection ~version)

let detach connection ~(device:int32) = castle_detach connection device
let detach_dev connection ~(device:string) = castle_detach connection (castle_device_to_devno device)

let create connection ~size = castle_create connection size

let int_of_destroy_flag = function
  | Destroy_tree -> 0l
  | Destroy_version -> 1l

let destroy connection ~version ~flag = castle_destroy connection version (int_of_destroy_flag flag)
let fault   connection ~(fault_id:int32) ~(fault_arg :int32)= castle_fault connection fault_id fault_arg

let slave_evacuate connection ~(disk:int32) ~(force:int32) = castle_slave_evacuate connection disk force
let slave_scan    connection ~id = castle_slave_scan connection id
let thread_priority  connection ~nice_value = castle_thread_priority connection nice_value

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

(* TODO *)
let transfer_create connection ~(version:int32) ~(disk:int32) = assert false
let transfer_destroy connection ~(transfer:int32) = assert false
let reserve_for_transfer connection ~(version:int32) ~(finished_only:bool) ~(constraints:(int32 * int32) list) = assert false
let get_valid_counts connection ~(slave:int32) = [| |] (* TODO not with an ioctl... *)
let get_invalid_counts connection ~(slave:int32) = [| |]
let set_target connection ~(slave:int32) ~value = assert false
