open Printf
open Unix
open FSTypes2

type connection

type rda_type =
    | RDA_1
    | RDA_2
    | SSD_RDA_2
    | SSD_RDA_3
    | META_EXT
    | MICRO_EXT
    | SUPER_EXT
    | SSD_ONLY_EXT
    | NR_RDA_SPECS

type merge_cfg = {
    m_arrays: int32 list;
    (* 'None' means merge all data extents (i.e. we pass -1 to Castle). *)
    m_data_exts: int64 list option;
    m_metadata_ext_type: rda_type;
    m_data_ext_type: rda_type;
    m_bandwidth: int32;
}

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
external castle_create_with_opts                : connection -> int64 -> int64 -> int32 = "caml_castle_create_with_opts"
external castle_vertree_tdp_set                 : connection -> int32 -> int64 -> unit = "caml_castle_vertree_compact"
(* NB additional function name is necessary since function has more than 5 params.
   Yes you read that right. See http://caml.inria.fr/pub/docs/manual-ocaml/manual032.html#htoc218.
   This also means ocaml-castle won't work with bytecode-interpreted OCaml programs
   until someone implements bytecode_bullshit in the C part. *)
external castle_merge_start                     : connection -> int32 -> int32 array -> int32 -> int64 array -> rda_type -> rda_type -> int32 -> int32 = "bytecode_bullshit" "caml_castle_merge_start"

let connect () =
        castle_connect ()

let disconnect connection = 
        castle_disconnect connection

let connection_fd conn = castle_fd conn

exception Invalid_reply of string
exception Invalid_iterator

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

let nimsg = "Not implemented in new interface but will be Soon™"
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

(* Disgusting HACK! See trac-3600. *)
let create connection ~size = castle_create_with_opts connection size 1L
let create_with_opts connection ~size ~opts = castle_create_with_opts connection size opts

let destroy_vertree connection ~vertree = castle_destroy_vertree connection vertree
let vertree_compact connection ~vertree = castle_vertree_compact connection vertree
let delete_version connection ~version = castle_delete_version connection version
let fault   connection ~(fault_id:int32) ~(fault_arg :int32)= castle_fault connection fault_id fault_arg

let slave_evacuate connection ~(disk:int32) ~(force:int32) = castle_slave_evacuate connection disk force
let slave_scan    connection ~id = castle_slave_scan connection id
let thread_priority  connection ~nice_value = castle_thread_priority connection nice_value
let ctrl_prog_deregister connection ~shutdown = castle_ctrl_prog_deregister connection shutdown
let vertree_tdp_set connection ~vertree ~seconds = castle_vertree_tdp_set connection vertree seconds

(* Here we unpack the OCaml values to make the C side of this function easier,
   and later construct the merge_cfg structure in C. *)
let merge_start connection ~merge_cfg =
    let arrays = Array.of_list merge_cfg.m_arrays in
    let arrays_length = Int32.of_int (Array.length arrays) in
    let data_exts, data_exts_length = match merge_cfg.m_data_exts with
        | Some l ->
            let arr = Array.of_list l in
            arr, Int32.of_int (Array.length arr)
        | None ->
            (* -1 for nr_data_exts means 'all'. *)
            [||], -1l
    in
    castle_merge_start
        connection
        arrays_length
        arrays
        data_exts_length
        data_exts
        merge_cfg.m_metadata_ext_type
        merge_cfg.m_data_ext_type
        merge_cfg.m_bandwidth

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
