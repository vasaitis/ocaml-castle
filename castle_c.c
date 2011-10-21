#include <sys/ioctl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <inttypes.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>

#include <caml/config.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/mlvalues.h>
#include <caml/fail.h>
#include <caml/signals.h>
#include <caml/callback.h>
#include <caml/unixsupport.h>
#include <caml/custom.h>

#include <castle/castle.h>

#undef DEBUG

//#define DEBUG
#ifndef DEBUG
#define debug(_f, ...)    ((void)0)
#else
#define debug(_f, _a...)  (printf(_f, ##_a))
#endif

#define Castle_val(v) (*(castle_connection **) Data_custom_val(v))

void caml_castle_finalize(value connection) {
  castle_free(Castle_val(connection));
}

struct custom_operations castle_ops = {
  .identifier = "com.acunu.castle",
  .finalize = &caml_castle_finalize,
  .compare = custom_compare_default,
  .hash = custom_hash_default,
  .serialize = custom_serialize_default,
  .deserialize = custom_deserialize_default,
};

// TODO use length to make sure we don't overrun
#define EMPTY_MEANS_NEGATIVE_INFINITY (-1)
#define EMPTY_MEANS_POSITIVE_INFINITY (1)
#define EMPTY_MEANS_EMPTY (0)
static void copy_ocaml_key_to_buffer(value key_value, void *buffer, uint32_t length, int empty_means)
{
    CAMLparam1(key_value);
    CAMLlocal1(subkey_value);

    uint32_t i;
    uint32_t dims = Wosize_val(key_value);
    int dim_len;
    int lens[dims];
    uint8_t flags[dims];
    const uint8_t *keys[dims];
    uint32_t r;

    /* We're expecting a list of strings */
    assert(Is_block(key_value) && Tag_val(key_value) == 0);

    for (i=0; i < dims; i++) {
      subkey_value = Field(key_value, i);

      assert(Is_block(subkey_value) && Tag_val(subkey_value) == String_tag);
      dim_len = lens[i]  = caml_string_length(subkey_value);

      if (dim_len == 0 && empty_means == EMPTY_MEANS_NEGATIVE_INFINITY) {
          flags[i] = KEY_DIMENSION_MINUS_INFINITY_FLAG;
      } else if (dim_len == 0 && empty_means == EMPTY_MEANS_POSITIVE_INFINITY) {
          flags[i] = KEY_DIMENSION_PLUS_INFINITY_FLAG;
      } else {
          flags[i] = 0;    
      }
      
      keys[i]  = (const uint8_t *)String_val(subkey_value);
    }

    r = castle_build_key(buffer, length, dims, lens, keys, flags);

    CAMLreturn0;
}

static void get_key_length(value key, uint32_t *length_out)
{
    CAMLparam1(key);
    CAMLlocal1(subkey_value);

    uint32_t i;
    uint32_t dims = Wosize_val(key);
    int lens[dims];
    uint32_t r;

    /* We're expecting a list of strings */
    assert(Is_block(key) && Tag_val(key) == 0);

    for (i=0; i < dims; i++) {
      subkey_value = Field(key, i);

      assert(Is_block(subkey_value) && Tag_val(subkey_value) == String_tag);
      lens[i] = caml_string_length(subkey_value);
    }

    r = castle_key_bytes_needed(dims, lens, NULL, NULL);
    *length_out = r;

    CAMLreturn0;
}

CAMLprim value caml_castle_connect(value unit)
{
    CAMLparam1(unit);
    CAMLlocal1(connection);

    castle_connection *conn;
    int ret;

    debug("fs_connect entered\n");

    ret = castle_connect(&conn);
    if (ret)
        unix_error(-ret, "castle_connect", Nothing);

    connection = caml_alloc_custom(&castle_ops, sizeof(conn), 1, 1);
    Castle_val(connection) = conn;

    debug("fs_connect exiting\n");

    CAMLreturn(connection);
}

CAMLprim void caml_castle_disconnect(value connection)
{
    CAMLparam1(connection);
    castle_connection *conn;

    debug("fs_disconnect entered\n");

    conn = Castle_val(connection);

    castle_disconnect(conn);

    debug("fs_disconnect exiting\n");

    CAMLreturn0;
}

#define MAX_GET_SIZE 512
CAMLprim value caml_castle_get(value connection, value collection, value key_value)
{
    CAMLparam3(connection, collection, key_value);
    CAMLlocal1(result);

    int ret;
    uint32_t key_len, val_len, collection_id;
    castle_connection *conn;
    castle_key *key;
    char *val;

    debug("fs_get entered\n");

    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    collection_id = Int32_val(collection);

    get_key_length(key_value, &key_len);
    key = malloc(key_len);
    if (!key) caml_failwith("Error allocating key");
    copy_ocaml_key_to_buffer(key_value, key, key_len, EMPTY_MEANS_EMPTY);

    enter_blocking_section();
    ret = castle_get(conn, collection_id, key, &val, &val_len);
    leave_blocking_section();

    free(key);

    if (ret)
    {
        switch (ret)
        {
            case -ENOENT:
                caml_raise_constant(*caml_named_value("acunuClientLib2 Not_found"));

            default:
                debug("Got error %d - '%s'", ret, strerror(ret));
                unix_error(-ret, "get", Nothing);
        }

        CAMLreturn(Val_unit); // If my assumptions are correct, we should never get here.
    }

    if (val_len == 0)
      result = Atom(String_tag);
    else {
      result = caml_alloc_string(val_len);
      memcpy(String_val(result), val, val_len);
    }
    free(val);

    debug("fs_get exiting\n");

    CAMLreturn(result);
}

CAMLprim void caml_castle_replace(value connection, value collection, value key_value, value val_value)
{
    CAMLparam4(connection, collection, key_value, val_value);

    int ret;
    uint32_t key_len, val_len, collection_id;
    castle_connection *conn;
    castle_key *key;
    void *buf;
    char *val;

    debug("fs_replace entered\n");

    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    collection_id = Int32_val(collection);

    get_key_length(key_value, &key_len);
    val_len = caml_string_length(val_value);

    buf = malloc(key_len + val_len);
    if (!buf)
    {
        debug("Could not alloc buffer.\n");
        caml_failwith("Could not alloc buffer.");
    }

    key = buf;
    val = buf + key_len;
    copy_ocaml_key_to_buffer(key_value, key, key_len, EMPTY_MEANS_EMPTY);
    memcpy(val, String_val(val_value), val_len);

    enter_blocking_section();
    ret = castle_replace(conn, collection_id, key, val, val_len);
    leave_blocking_section();
    free(buf);
    if (ret)
    {
        debug("Got error %d - '%s'", ret, strerror(ret));
        unix_error(-ret, "replace", Nothing);
    }

    CAMLreturn0;
}

CAMLprim void caml_castle_remove(value connection, value collection, value key_value)
{
    CAMLparam3(connection, collection, key_value);

    int ret;
    uint32_t key_len, collection_id;
    castle_connection *conn;
    castle_key *key;

    debug("fs_remove entered\n");

    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    collection_id = Int32_val(collection);

    get_key_length(key_value, &key_len);

    key = malloc(key_len);
    if (!key)
    {
        debug("Could not alloc buffer.\n");
        caml_failwith("Could not alloc buffer.");
    }

    copy_ocaml_key_to_buffer(key_value, key, key_len, EMPTY_MEANS_EMPTY);

    enter_blocking_section();
    ret = castle_remove(conn, collection_id, key);
    leave_blocking_section();
    free(key);
    if (ret)
    {
        debug("Got error %d - '%s'", ret, strerror(ret));
        unix_error(-ret, "remove", Nothing);
    }

    CAMLreturn0;
}

static value castle_key_to_ocaml(castle_key *key)
{
    CAMLparam0();
    CAMLlocal2(ocaml_key, ocaml_key_dim);
    uint32_t i;
    uint32_t dimension_length;
    const uint8_t *dimension;

    if (key->nr_dims == 0)
      CAMLreturn(Atom(0));

    ocaml_key = caml_alloc(key->nr_dims, 0);

    for (i = 0; i < castle_key_dims(key); i++)
    {
        dimension_length = castle_key_elem_len(key, i);
        dimension = castle_key_elem_data(key, i);

        if (dimension_length == 0)
            Store_field(ocaml_key, i, Atom(String_tag));
        else {
            ocaml_key_dim = caml_alloc_string(dimension_length);
            memcpy(String_val(ocaml_key_dim), dimension, dimension_length);
            Store_field(ocaml_key, i, ocaml_key_dim);
        }
    }

    CAMLreturn(ocaml_key);
}

static value castle_kv_list_to_ocaml(struct castle_key_value_list *kv_list)
{
    CAMLparam0();
    CAMLlocal3(arr, kv_tuple, ocaml_val_str);

    uint32_t i = 0, key_count = 0;
    struct castle_key_value_list *cur_kv_list;

    /* find number of kv pairs */
    cur_kv_list = kv_list;
    while (cur_kv_list)
    {
        key_count++;
        cur_kv_list = cur_kv_list->next;
    }

    debug("castle_kv_list_to_ocaml got %u keys\n", key_count);

    if (key_count == 0)
        CAMLreturn(Atom(0));

    arr = caml_alloc(key_count, 0);

    /* insert items into array */
    while (kv_list)
    {
        kv_tuple = caml_alloc(2, 0);
        Store_field(kv_tuple, 0, castle_key_to_ocaml(kv_list->key));
        if (kv_list->val->length == 0)
          Store_field(kv_tuple, 1, Atom(String_tag));
        else {
          ocaml_val_str = caml_alloc_string(kv_list->val->length);
          memcpy(String_val(ocaml_val_str), kv_list->val->val, kv_list->val->length);
          Store_field(kv_tuple, 1, ocaml_val_str);
        }

        assert(i < key_count);
        Store_field(arr, i, kv_tuple);
        i++;

        kv_list = kv_list->next;
    }

    CAMLreturn(arr);
}

CAMLprim value caml_castle_iter_start(value connection, value collection, value start_key, value end_key, value size)
{
    CAMLparam5(connection, collection, start_key, end_key, size);
    CAMLlocal3(token_out, arr, ret_tuple);

    int ret, more;
    uint32_t start_key_len, end_key_len, collection_id, buf_length;
    void *start_key_buf, *end_key_buf;
    struct castle_key_value_list *kv_list;
    castle_connection *conn;
    castle_interface_token_t token;

    debug("fs_iter_start entered\n");

    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    buf_length = Int_val(size);
    collection_id = Int32_val(collection);

    get_key_length(start_key, &start_key_len);
    get_key_length(end_key, &end_key_len);

    start_key_buf = malloc(start_key_len);
    end_key_buf = malloc(end_key_len);
    if (!start_key_buf || !end_key_buf)
    {
        debug("Could not alloc buffer.\n");
        caml_failwith("Could not alloc buffer.");
    }

    copy_ocaml_key_to_buffer(start_key, start_key_buf, start_key_len, EMPTY_MEANS_NEGATIVE_INFINITY);
    copy_ocaml_key_to_buffer(end_key, end_key_buf, end_key_len, EMPTY_MEANS_POSITIVE_INFINITY);

    enter_blocking_section();
    ret = castle_iter_start(conn,
                            collection_id,
                            start_key_buf,
                            end_key_buf,
                            &token,
                            &kv_list,
                            buf_length,
                            &more);
    leave_blocking_section();

    free(start_key_buf);
    free(end_key_buf);

    if (ret)
        unix_error(-ret, "iter_start", Nothing);

    ret_tuple = caml_alloc(3, 0);
    Store_field(ret_tuple, 0, caml_copy_int32(token));
    Store_field(ret_tuple, 1, more ? Val_int(1) : Val_int(0));
    Store_field(ret_tuple, 2, castle_kv_list_to_ocaml(kv_list));

    castle_kvs_free(kv_list);

    debug("fs_iter_start exiting\n");

    CAMLreturn(ret_tuple);
}

CAMLprim value caml_castle_iter_next(value connection, value token, value size)
{
    CAMLparam3(connection, token, size);
    CAMLlocal2(arr, ret_tuple);

    int ret, more;
    uint32_t buf_length;

    castle_interface_token_t token_id;
    struct castle_key_value_list *kv_list;
    castle_connection *conn;

    debug("fs_iter_next entered\n");

    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    buf_length = Int_val(size);
    token_id = Int32_val(token);

    enter_blocking_section();
    ret = castle_iter_next(conn, token_id, &kv_list, buf_length, &more);
    leave_blocking_section();
    if (ret)
    {
        unix_error(-ret, "iter_next", Nothing);
    }
    arr = castle_kv_list_to_ocaml(kv_list);
    castle_kvs_free(kv_list);

    ret_tuple = caml_alloc(2, 0);
    Store_field(ret_tuple, 0, more ? Val_int(1) : Val_int(0));
    Store_field(ret_tuple, 1, arr);

    debug("fs_iter_next exiting\n");

    CAMLreturn(ret_tuple);
}

CAMLprim void caml_castle_iter_finish(value connection, value token)
{
    CAMLparam2(connection, token);

    int ret;
    castle_connection *conn;

    debug("fs_iter_finish entered\n");

    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    enter_blocking_section();
    ret = castle_iter_finish(conn, Int32_val(token));
    leave_blocking_section();
    if (ret)
    {
        unix_error(-ret, "iter_finish", Nothing);
    }

    debug("fs_iter_finish exiting\n");

    CAMLreturn0;
}

CAMLprim value caml_castle_get_slice(value connection, value collection, value from_key_value, value to_key_value, value limit)
{
    CAMLparam5(connection, collection, from_key_value, to_key_value, limit);
    CAMLlocal1(result);

    int ret;
    uint32_t from_key_len, to_key_len, collection_id;
    castle_connection *conn;
    struct castle_key_value_list *kvs;
    castle_key *from_key, *to_key;
    void *buf;

    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    collection_id = Int32_val(collection);

    get_key_length(from_key_value, &from_key_len);
    get_key_length(to_key_value, &to_key_len);

    buf = malloc(from_key_len + to_key_len);
    if (!buf) caml_failwith("Error allocating key");
    from_key = buf;
    to_key = buf + from_key_len;
    copy_ocaml_key_to_buffer(from_key_value, from_key, from_key_len, EMPTY_MEANS_NEGATIVE_INFINITY);
    copy_ocaml_key_to_buffer(to_key_value, to_key, to_key_len, EMPTY_MEANS_POSITIVE_INFINITY);

    enter_blocking_section();
    ret = castle_getslice(conn, collection_id, from_key,
        to_key, &kvs, Int_val(limit));
    leave_blocking_section();

    free(buf);

    if (ret)
    {
        debug("Got error %d - '%s'", ret, strerror(ret));
        unix_error(-ret, "getslice", Nothing);
        CAMLreturn(Val_unit); // If my assumptions are correct, we should never get here.
    }

    result = castle_kv_list_to_ocaml(kvs);

    castle_kvs_free(kvs);

    CAMLreturn(result);
}

/* IOCTLS */

#define CAML_VAL_slave_uuid Int32_val
#define CAML_VAL_collection_id Int32_val
#define CAML_VAL_version Int32_val
#define CAML_VAL_uint8 Bool_val
#define CAML_VAL_uint32 Int32_val
#define CAML_VAL_uint64 Int64_val
#define CAML_VAL_size Int_val
#define CAML_VAL_string String_val
#define CAML_VAL_int32 Int32_val
#define CAML_VAL_da_id_t Int32_val
#define CAML_VAL_merge_id_t Int32_val
#define CAML_VAL_thread_id_t Int32_val
#define CAML_VAL_work_id_t Int32_val
#define CAML_VAL_work_size_t Int64_val
#define CAML_VAL_pid Int32_val

#define CAML_COPY_slave_uuid caml_copy_int32
#define CAML_COPY_collection_id caml_copy_int32
#define CAML_COPY_version caml_copy_int32
#define CAML_COPY_uint32 caml_copy_int32
#define CAML_COPY_string caml_copy_stri64
#define CAML_COPY_int32 caml_copy_int32
#define CAML_COPY_da_id_t caml_copy_int32
#define CAML_COPY_merge_id_t caml_copy_int32
#define CAML_COPY_thread_id_t caml_copy_int32
#define CAML_COPY_work_id_t caml_copy_int32
#define CAML_COPY_work_size_t caml_copy_int64
#define CAML_COPY_pid caml_copy_int32

#define CASTLE_IOCTL_0IN_0OUT(_id, _name)                                           \
CAMLprim void                                                                       \
caml_castle_##_id (value connection)                                                     \
{                                                                                   \
        CAMLparam1(connection);                                                     \
        castle_connection *conn;                                       \
        int ret;                                                                    \
                                                                                    \
        assert(Is_block(connection) && Tag_val(connection) == Custom_tag);        \
        conn = Castle_val(connection);                                  \
                                                                                    \
        enter_blocking_section();                                                   \
        ret = castle_##_id(conn);                                             \
        leave_blocking_section();                                                   \
                                                                                    \
        if (ret)                                                                    \
            unix_error(-ret, #_id, Nothing);                                         \
                                                                                    \
        CAMLreturn0;                                                                \
}                                                                                   \

#define CASTLE_IOCTL_0IN_1OUT(_id, _name, _ret_1_t, _ret)                           \
CAMLprim value                                                                      \
caml_castle_##_id (value connection)                                                \
{                                                                                   \
        CAMLparam1(connection);                                                     \
        CAMLlocal1(result);                                                         \
        castle_connection *conn;                                                    \
        int ret;                                                                    \
        C_TYPE_##_ret_1_t _ret;                                                     \
                                                                                    \
        assert(Is_block(connection) && Tag_val(connection) == Custom_tag);          \
        conn = Castle_val(connection);                                              \
                                                                                    \
        enter_blocking_section();                                                   \
        ret = castle_##_id(conn, &_ret);                                            \
        leave_blocking_section();                                                   \
                                                                                    \
        if (ret)                                                                    \
            unix_error(-ret, #_id, Nothing);                                        \
                                                                                    \
        result = CAML_COPY_##_ret_1_t(_ret);                                        \
                                                                                    \
        CAMLreturn(result);                                                         \
}                                                                                   \

#define CASTLE_IOCTL_1IN_0OUT(_id, _name, _arg_1_t, _arg_1)                         \
CAMLprim void                                                                       \
caml_castle_##_id (value connection, value _arg_1##_value)                               \
{                                                                                   \
        CAMLparam2(connection, _arg_1##_value);                                     \
        castle_connection *conn;                                       \
        int ret;                                                                    \
        C_TYPE_##_arg_1_t _arg_1;                                                   \
                                                                                    \
        assert(Is_block(connection) && Tag_val(connection) == Custom_tag);        \
        conn = Castle_val(connection);                                  \
                                                                                    \
        _arg_1 = CAML_VAL_##_arg_1_t(_arg_1##_value);                               \
                                                                                    \
        enter_blocking_section();                                                   \
        ret = castle_##_id(conn, _arg_1);                                     \
        leave_blocking_section();                                                   \
                                                                                    \
        if (ret)                                                                    \
            unix_error(-ret, #_id, Nothing);                                         \
                                                                                    \
        CAMLreturn0;                                                                \
}                                                                                   \

#define CASTLE_IOCTL_1IN_1OUT(_id, _name, _arg_1_t, _arg_1, _ret_1_t, _ret)         \
CAMLprim value                                                                      \
caml_castle_##_id (value connection, value _arg_1##_value)              \
{                                                                                   \
        CAMLparam2(connection, _arg_1##_value);                                     \
        CAMLlocal1(result);                                                         \
        castle_connection *conn;                                       \
        int ret;                                                                    \
        C_TYPE_##_arg_1_t _arg_1;                                                   \
        C_TYPE_##_ret_1_t _ret;                                                     \
                                                                                    \
        assert(Is_block(connection) && Tag_val(connection) == Custom_tag);        \
        conn = Castle_val(connection);                                  \
                                                                                    \
        _arg_1 = CAML_VAL_##_arg_1_t(_arg_1##_value);                               \
                                                                                    \
        enter_blocking_section();                                                   \
        ret = castle_##_id(conn, _arg_1, &_ret);                              \
        leave_blocking_section();                                                   \
                                                                                    \
        if (ret)                                                                    \
            unix_error(-ret, #_id, Nothing);                                         \
                                                                                    \
        result = CAML_COPY_##_ret_1_t(_ret);                                        \
                                                                                    \
        CAMLreturn(result);                                                         \
}                                                                                   \

#define CASTLE_IOCTL_2IN_0OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2)       \
CAMLprim void                                                                       \
caml_castle_##_id (value connection, value _arg_1##_value, value _arg_2##_value)    \
{                                                                                   \
        CAMLparam3(connection, _arg_1##_value, _arg_2##_value);                     \
        castle_connection *conn;                                       \
        int ret;                                                                    \
        C_TYPE_##_arg_1_t _arg_1;                                                   \
        C_TYPE_##_arg_2_t _arg_2;                                                   \
                                                                                    \
        assert(Is_block(connection) && Tag_val(connection) == Custom_tag);        \
        conn = Castle_val(connection);                                  \
                                                                                    \
        _arg_1 = CAML_VAL_##_arg_1_t(_arg_1##_value);                               \
        _arg_2 = CAML_VAL_##_arg_2_t(_arg_2##_value);                               \
                                                                                    \
        enter_blocking_section();                                                   \
        ret = castle_##_id(conn, _arg_1, _arg_2);                                   \
        leave_blocking_section();                                                   \
                                                                                    \
        if (ret)                                                                    \
            unix_error(-ret, #_id, Nothing);                                         \
                                                                                    \
        CAMLreturn0;                                                                \
}                                                                                   \

#define CASTLE_IOCTL_2IN_1OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2,       \
                              _ret_1_t, _ret)                                       \
CAMLprim value                                                                      \
caml_castle_##_id (value connection, value _arg_1##_value, value _arg_2##_value)    \
{                                                                                   \
        CAMLparam3(connection, _arg_1##_value, _arg_2##_value);                     \
        CAMLlocal1(result);                                                         \
        castle_connection *conn;                                                    \
        int ret;                                                                    \
        C_TYPE_##_arg_1_t _arg_1;                                                   \
        C_TYPE_##_arg_2_t _arg_2;                                                   \
        C_TYPE_##_ret_1_t _ret;                                                     \
                                                                                    \
        assert(Is_block(connection) && Tag_val(connection) == Custom_tag);          \
        conn = Castle_val(connection);                                              \
                                                                                    \
        _arg_1 = CAML_VAL_##_arg_1_t(_arg_1##_value);                               \
        _arg_2 = CAML_VAL_##_arg_2_t(_arg_2##_value);                               \
                                                                                    \
        enter_blocking_section();                                                   \
        ret = castle_##_id(conn, _arg_1, _arg_2, &_ret);                            \
        leave_blocking_section();                                                   \
                                                                                    \
        if (ret)                                                                    \
            unix_error(-ret, #_id, Nothing);                                        \
                                                                                    \
        result = CAML_COPY_##_ret_1_t(_ret);                                        \
                                                                                    \
        CAMLreturn(result);                                                         \
}                                                                                   \

#define CASTLE_IOCTL_3IN_1OUT(_id, _name, _arg_1_t, _arg_1, _arg_2_t, _arg_2,       \
    _arg_3_t, _arg_3, _ret_1_t, _ret)

CASTLE_IOCTLS

CAMLprim value
caml_castle_device_to_devno(value filename) {
  CAMLparam1(filename);
  CAMLlocal1(result);

  size_t filename_len = caml_string_length(filename) + 1;
  char filename_buf[filename_len];
  uint32_t devno;
  memcpy(filename_buf, String_val(filename), filename_len);
  filename_buf[filename_len] = '\0';

  enter_blocking_section();
  devno = castle_device_to_devno(filename_buf);
  leave_blocking_section();

  result = caml_copy_int32(devno);
  CAMLreturn(result);
}

CAMLprim value
caml_castle_devno_to_device(value devno_v) {
  CAMLparam1(devno_v);
  CAMLlocal1(result);

  uint32_t devno = Int32_val(devno_v);
  const char *filename;

  enter_blocking_section();
  filename = castle_devno_to_device(devno);
  leave_blocking_section();

  result = caml_copy_string(filename);
  CAMLreturn(result);
}

CAMLprim value
caml_castle_collection_attach (value connection, value version_v, value name_v)
{
        CAMLparam3(connection, version_v, name_v);
        CAMLlocal1(result);
        castle_connection *conn;
        int ret;

        c_ver_t version = Int32_val(version_v);
        size_t name_len = caml_string_length(name_v) + 1;
        char *name = malloc(name_len);

        c_collection_id_t collection;

        assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
        conn = Castle_val(connection);

        memcpy(name, String_val(name_v), name_len);

        enter_blocking_section();
        ret = castle_collection_attach(conn, version, name, name_len, &collection);
        leave_blocking_section();

        free(name);

        if (ret)
            unix_error(-ret, "collection_attach", Nothing);

        result = caml_copy_int32(collection);

        CAMLreturn(result);
}

CAMLprim void
caml_castle_environment_set(value connection, value val_id, value data_v) {
  CAMLparam3(connection, val_id, data_v);
  castle_connection *conn;

  castle_env_var_id id = Int32_val(val_id);
  size_t data_len = caml_string_length(data_v) + 1;
  char *data = malloc(data_len);
  int ign;

  assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
  conn = Castle_val(connection);

  memcpy(data, String_val(data_v), data_len);

  enter_blocking_section();
  castle_environment_set(conn, id, data, data_len, &ign);
  leave_blocking_section();

  free(data);

  CAMLreturn0;
}

CAMLprim value
caml_castle_merge_start(
        value connection,
        value arrays_length,
        value arrays,
        value data_exts_length,
        value data_exts,
        value metadata_ext_type,
        value data_ext_type,
        value bandwidth)
{
    CAMLparam5(connection, arrays_length, arrays, data_exts_length, data_exts);
    CAMLxparam3(metadata_ext_type, data_ext_type, bandwidth);
    CAMLlocal1(result);

    castle_connection *conn;
    assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
    conn = Castle_val(connection);

    /* Now pack the params into a C merge_cfg */
    c_merge_cfg_t merge_cfg;
    /* Copy OCaml Int32 array -> C array */
    merge_cfg.nr_arrays = Int32_val(arrays_length);
    merge_cfg.arrays = malloc(sizeof(merge_cfg.arrays[0]) * merge_cfg.nr_arrays);
    for (int i = 0; i < merge_cfg.nr_arrays; i++)
        merge_cfg.arrays[i] = Int32_val(Field(arrays, i));
    /* Copy OCaml Int64 array -> C array */
    merge_cfg.nr_data_exts = Int32_val(data_exts_length);
    merge_cfg.data_exts = malloc(sizeof(merge_cfg.data_exts[0]) * merge_cfg.nr_data_exts);
    for (int i = 0; i < merge_cfg.nr_data_exts; i++)
        merge_cfg.data_exts[i] = Int64_val(Field(data_exts, i));
    /* Other bits */
    merge_cfg.metadata_ext_type = Int_val(metadata_ext_type);
    merge_cfg.data_ext_type = Int_val(data_ext_type);
    merge_cfg.bandwidth = Int32_val(bandwidth);

    enter_blocking_section();
    c_merge_id_t merge_id;
    int ret = castle_merge_start(conn, merge_cfg, &merge_id);
    leave_blocking_section();

    if (ret)
        unix_error(-ret, "merge_start", Nothing);

    result = caml_copy_int32(merge_id);
    CAMLreturn(result);
}

CAMLprim value
caml_castle_fd(value connection) {
  CAMLparam1(connection);
  castle_connection *conn;

  assert(Is_block(connection) && Tag_val(connection) == Custom_tag);
  conn = Castle_val(connection);

  int fd = castle_fd(conn);
  CAMLreturn(Val_int(fd));
}
