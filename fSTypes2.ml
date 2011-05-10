
type device_id = int32

type transfer_id = int32

type version_id = int32

type collection_id = int32

type disk_id = int32

type obj_collection = string  (* i.e. collection name *)
type obj_key = string array
type obj_value =
    | Tombstone
    | Value of string

type iter_token = int32
type iter_index = int32

type environment_var_id =
  | BuildId
  | ModuleHash
  | Description
  | Hostname

type destroy_flag =
  | Destroy_tree
  | Destroy_version
