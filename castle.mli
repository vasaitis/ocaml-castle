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
type castle_state =
  | CASTLE_STATE_LOADING
  | CASTLE_STATE_UNINITED
  | CASTLE_STATE_INITED
val string_of_castle_state :
  castle_state ->
  string
type merge_cfg = {
  m_arrays : int32 list;
  m_data_exts : int64 list option;
  m_metadata_ext_type : rda_type;
  m_data_ext_type : rda_type;
  m_bandwidth : int32;
}
val connect : unit -> connection
val disconnect : connection -> unit
val connection_fd : connection -> Unix.file_descr
exception Invalid_reply of string
exception Invalid_iterator
exception Castle_not_running
val get :
  connection -> FSTypes2.collection_id -> FSTypes2.obj_key -> FSTypes2.obj_value
val get_slice :
  connection ->
  FSTypes2.collection_id ->
  FSTypes2.obj_key ->
  FSTypes2.obj_key -> int -> (FSTypes2.obj_key * FSTypes2.obj_value) array
val replace :
  connection -> FSTypes2.collection_id -> ?timestamp:int64 -> FSTypes2.obj_key -> string -> unit
val multi_replace :
  connection -> FSTypes2.collection_id -> (FSTypes2.obj_key * string) array -> unit
val remove : connection -> FSTypes2.collection_id -> ?timestamp:int64 -> FSTypes2.obj_key -> unit
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
val create_with_opts : connection -> size:int64 -> opts:int64 -> int32
val destroy_vertree : connection -> vertree:int32 -> unit
val vertree_compact : connection -> vertree:int32 -> unit
val delete_version : connection -> version:int32 -> unit
val clone : connection -> version:int32 -> int32
val snapshot : connection -> device:int32 -> int32
val snapshot_dev : connection -> device:string -> int32
val init : connection -> unit
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
val ctrl_prog_deregister : connection -> shutdown:bool -> int32
val merge_start : connection -> merge_cfg:merge_cfg -> int32
val vertree_tdp_set : connection -> vertree:int32 -> seconds:int64 -> unit
val state_query : connection -> castle_state
