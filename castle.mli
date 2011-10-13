type connection
val connect : string -> int -> int -> connection
val disconnect : connection -> unit
val connection_fd : connection -> Unix.file_descr
exception Invalid_reply of string
exception Invalid_iterator
val lib_init : unit -> unit
val get :
  connection -> FSTypes2.collection_id -> FSTypes2.obj_key -> FSTypes2.obj_value
val get_slice :
  connection ->
  FSTypes2.collection_id ->
  FSTypes2.obj_key ->
  FSTypes2.obj_key -> int -> (FSTypes2.obj_key * FSTypes2.obj_value) array
val replace :
  connection -> FSTypes2.collection_id -> FSTypes2.obj_key -> string -> unit
val multi_replace :
  connection -> FSTypes2.collection_id -> (FSTypes2.obj_key * string) array -> unit
val remove : connection -> FSTypes2.collection_id -> FSTypes2.obj_key -> unit
val iter_start :
  connection ->
  FSTypes2.collection_id ->
  FSTypes2.obj_key ->
  FSTypes2.obj_key ->
  int ->
  FSTypes2.iter_token * bool * ((FSTypes2.obj_key * FSTypes2.obj_value) array)
val iter_next :
  connection ->
  FSTypes2.iter_token ->
  int ->
  bool * ((FSTypes2.obj_key * FSTypes2.obj_value) array)
val iter_replace_last :
  connection -> FSTypes2.iter_token -> FSTypes2.iter_index -> string -> unit
val iter_finish : connection -> FSTypes2.iter_token -> unit
val claim : connection -> device:int32 -> int32
val claim_dev : connection -> device:string -> int32
val attach : connection -> version:FSTypes2.version_id -> int32
val attach_dev : connection -> version:FSTypes2.version_id -> string
val detach : connection -> device:int32 -> unit
val detach_dev : connection -> device:string -> unit
val create : connection -> size:int64 -> int32
val destroy_vertree : connection -> vertree:int32 -> unit
val vertree_compact : connection -> vertree:int32 -> unit
val delete_version : connection -> version:int32 -> unit
val clone : connection -> version:int32 -> int32
val snapshot : connection -> device:int32 -> int32
val snapshot_dev : connection -> device:string -> int32
val init : connection -> unit
val transfer_create : connection -> version:int32 -> disk:int32 -> int32
val transfer_destroy : connection -> transfer:int32 -> unit
val collection_attach :
  connection -> version:int32 -> name:FSTypes2.obj_collection -> int32
val collection_reattach :
  connection -> collection:int32 -> new_version:int32 -> unit
val collection_detach : connection -> collection:int32 -> unit
val collection_take_snapshot : connection -> collection:int32 -> int32
val environment_set : connection -> FSTypes2.environment_var_id -> string -> unit
val fault : connection -> fault_id:int32 -> fault_arg:int32 -> unit
val slave_evacuate : connection -> disk:int32 -> force:int32 -> unit
val slave_scan : connection -> id:int32 -> unit
val thread_priority : connection -> nice_value:int32 -> unit
val reserve_for_transfer :
  connection ->
  version:int32 ->
  finished_only:bool -> constraints:(int32 * int32) list -> unit
val get_valid_counts :
  connection -> slave:int32 -> (FSTypes2.version_id * int32) array
val get_invalid_counts :
  connection -> slave:int32 -> (FSTypes2.version_id * int32) array
val set_target : connection -> slave:int32 -> value:bool -> unit
val ctrl_prog_deregister : connection -> shutdown:bool -> int32
