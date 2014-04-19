open Str
open OUnit

module Subscriptions : sig

    type 'a t

    val new_node : string -> 'a t

    val add_node : 'a t ->  string -> 'a -> 'a t

    val remove_node : 'a t -> string ->'a -> 'a t

    val query : 'a t -> string -> 'a list

    val tests : OUnit.test list

end = struct

type node = Key of string | Plus | Pound
type 'a t = Node of node * 'a list * 'a t list

let default x = function
    None -> x
    | Some y -> y

let get_key = function
    | Node(x, _, _) -> x

let get_value = function
    | Node(_, x, _) -> x

let get_branches = function
    | Node(_, _, x) -> x

let set_value value node =
    let Node (key, v_list, branches) = node in
    Node(key, value :: v_list, branches)

let filter_branches elem branches =
    let cmp node =
        match get_key node with
            | Key y -> 0 == String.compare elem y
            | Plus | Pound -> true in
    List.filter cmp branches

let concat parms node =
    match get_key node with
    | Key _ | Plus -> parms
    | Pound -> parms @ get_value node

let ugly parms elem =
    let x = parms @ get_value elem in
    try
        let cmp_pound elem =
            match get_key elem with
            | Pound -> true
            | _ -> false in
        let branches = get_branches elem in
        let p = List.find cmp_pound branches in
        x @ get_value p
    with Not_found -> x

let query tree key =
    let k_parts = split (regexp "/") key in
    let rec inner tree parms = function
    | [] -> ugly parms tree
    | h::t ->
        let branches = get_branches tree in
        let nodes = filter_branches h branches in
        let r = List.map (fun x -> inner x [] t) nodes in
        concat parms tree @ List.concat r in
    inner tree [] k_parts

let new_node elem =
    let res = ( elem = "+", elem = "#" ) in
    let inner = match res with
        | (true, _) -> Plus
        | (_, true) -> Pound
        | _ -> Key elem in
    Node (inner, [], [])

let replace_branch branch tree =
    let a = get_key branch in
    let Node (v, q, branches) = tree in
    let filt node =
        let b = get_key node in
        match a, b with
            | Plus, Plus
            | Pound, Pound -> false
            | Key c, Key d -> 0 != String.compare c d
            | _, _ -> true in
    let sans_a = List.filter filt branches in
    Node (v, q, branch::sans_a)

let find_node elem branches =
    let cmp node =
        match get_key node with
            | Plus -> elem = "+"
            | Pound -> elem = "#"
            | Key k -> 0 == String.compare elem k in
    try Some (List.find cmp branches) with Not_found -> None

let rec build tree parts value =
    let branches = get_branches tree in
    match parts with
    | [] -> tree
    | h :: [] ->
        let n = default (new_node h) (find_node h branches) in
        let q = set_value value n in
        replace_branch q tree
    | h::t -> let n = default (new_node h) (find_node h branches) in
        replace_branch (build n t value) tree

let add_node tree str value =
    let parts = split (regexp "/") str in
    build tree parts value

let rec children_have_values node =
    let cmp acc x = if acc then acc else children_have_values x in
    if 0 = List.length (get_branches node) &&
       0 != List.length (get_value node) then true
    else List.fold_left cmp false (get_branches node)

let rec remove tree parts value =
    match parts with
    | h :: t -> begin
        match find_node h (get_branches tree) with
            | None -> tree
            | Some s ->
                let not_wild = h <> "+" && h <> "#" in
                let Node (k, v, b) = tree in
                let nb = List.map (fun x -> remove x t value) b in
                let nb = List.filter children_have_values nb in
                let nv =
                    if not_wild then List.filter (fun x -> x <> value) v
                    else v in
                Node (k, nv, nb)
        end
    | [] ->
        let v = List.filter (fun x -> x <> value) (get_value tree) in
        Node (get_key tree, v, get_branches tree)

let remove_node tree str value =
    let parts = split (regexp "/") str in
    remove tree parts value

let plus_test _ =
    let tree = new_node "" in
    let tree = add_node tree "helo/happy/world!" "helosadworld" in
    let tree = add_node tree "helo/pretty/wurld" "helothere" in
    let tree = add_node tree "omg/wtf" "omgwtf" in
    let tree = add_node tree "omg/wtf/bbq" "omgwtfbbq" in
    let tree = add_node tree "omg/srsly" "omgsrsly" in
    let tree = add_node tree "omg/+/bbqz" "omgwildbbq" in
    let tree = add_node tree "omg/+" "omgwildcard" in
    let tree = add_node tree "a/+/c/+/e/+" "abcdef" in
    let tree = add_node tree "asdfies" "QWERTIES" in
    let r = [
        "helo/pretty/wurld";
        "helo/heh/qwert/blah";
        "omg/lol/bbqz";
        "helo/pretty";
        "omg/wtf";
        "omg/wtf!";
        "a/lol/c/def/e/orly";
        "alol/c/def/e";
    ] in
    let expected = [
        ["helothere"];
        [];
        ["omgwildbbq"];
        [];
        ["omgwildcard"; "omgwtf"];
        ["omgwildcard"];
        ["abcdef"];
        [];
    ] in
    let printer = String.concat "," in
    let res = List.map (fun x -> query tree x) r in
    List.iter2 (fun x y -> assert_equal ~printer x y) expected res

let pound_test _ =
    let root = new_node "#" in
    let root = add_node root "a/#" "a" in
    let root = add_node root "a/b/#" "ab" in
    let root = add_node root "a/b" "plainab" in
    let root = add_node root "a/b/c/#" "abc" in
    let root = add_node root "a/b/c/d/#" "abcd" in
    let root = add_node root "a/b/c/d/e/#" "abcde" in
    let r = [
        "a";
        "a/b";
        "a/b/c";
        "a/c";
        "d/e";
    ] in
    let expected = [
        ["a"];
        ["plainab"; "ab"; "a"];
        ["abc"; "ab"; "a"];
        ["a"];
        [];
    ] in
    let printer = String.concat "," in
    let res = List.map (fun x -> query root x) r in
    List.iter2 (fun x y -> assert_equal ~printer x y) expected res

let rec tree_of_string tree level =
    let Node (v, q, branches) = tree in
    let r = match v with Key k -> k | Pound -> "#" | Plus -> "+" in
    let s = String.concat "," q in
    let s = if String.length s > 0 then ": " ^ s else s in
    let _ = Printf.printf "%*d %s %s\n" level level r s in
    let func x = tree_of_string x (level + 4) in
    List.iter func branches

let remove_test _ =
    let root = new_node "*root*" in
    let root = add_node root "a"   "a" in
    let root = add_node root "a/+" "a+" in
    let root = add_node root "a/#" "a#" in
    let root = add_node root "a/#" "tst" in
    let root = add_node root "a"  "tst" in (* to double check wilds *)
    let root = add_node root "a/b" "ab" in
    let root = add_node root "a/b/#" "ab#" in
    let root = add_node root "a/b"  "ab2" in
    let root = add_node root "a/+/c" "a+c" in
    let root = add_node root "a/b/c" "abc" in
    let root = add_node root "a/+/c/#" "a+c#" in
    let root = add_node root "a/b/c/d" "abcd" in
    let root = add_node root "a/b/c/d" "abcd2" in
    let printer = String.concat ", " in
    let cmp a b =
        let s = List.sort (fun x y -> String.compare x y) in
        (s a) = (s b) in
    let ae = assert_equal ~printer ~cmp in

    (*  really should modify to return the fully qualified key
        of the empty node
    *)
    let rec has_empty_leaves tree =
        let check acc x =
            if acc then acc
            else has_empty_leaves x in
        if  0 = List.length (get_branches tree) &&
            0 = List.length (get_value tree) then true
        else List.fold_left check false (get_branches tree) in

    (* sanity check the first element *)
    let res = query root "a" in
    ae ["tst"; "a"; "tst"; "a#";] res;
    let root = remove_node root "a/#" "tst" in
    let res = query root "a" in
    ae ["tst"; "a"; "a#";] res;
    let root = remove_node root "a" "tst" in
    let res = query root "a" in
    ae ["a"; "a#";] res;

    (* check that there are no empty leaves *)
    let res = query root "a/b" in
    ae ["ab"; "ab#"; "ab2"; "a#"; "a+";] res;
    (* should produce an empty leaf; the "#" dangles *)
    let root = remove_node root "a/b/#" "ab#" in
    let res = query root "a/b" in
    ae ["ab"; "ab2"; "a#"; "a+"] res;
    assert_equal ~msg: "empty leaves a/b/#" false (has_empty_leaves root);

    (* misc things, such as removing all values from a parent *)
    let res = query root "a/b/c" in
    ae ["a+c"; "abc"; "a+c#"; "a#"] res;
    let root = remove_node root "a/b/c" "abc" in
    let res = query root "a/b/c" in
    ae ["a+c"; "a+c#"; "a#"] res;
    let root = remove_node root "a/+/c" "a+c" in
    let res = query root "a/b/c" in
    ae ["a+c#"; "a#";] res;
    let root = remove_node root "a/+/c/#" "a+c#" in
    let res = query root "a/b/c" in
    ae ["a#"] res;
    assert_equal ~msg:"empty leaves; none" false (has_empty_leaves root);

    (* remove a sibling value from same key *)
    let res = query root "a/b/c/d" in
    ae ["a#"; "abcd"; "abcd2"] res;
    let root = remove_node root "a/b/c/d" "abcd" in
    let res = query root "a/b/c/d" in
    ae ["a#"; "abcd2"] res;

    (* remove nonexistent value from valid key *)
    let root = remove_node root "a/b/c/d" "nothere" in
    let res = query root "a/b/c/d" in
    ae ["a#"; "abcd2"] res;

    (* remove nonexistent value from invalid key *)
    let root = remove_node root "a/c/d/" "rlynothere" in
    let res = query root "a/c/d" in
    ae ["a#"] res;
    assert_equal ~msg: "empty leaves; final" false (has_empty_leaves root);
    ()

let tests = [
    "plus test" >:: plus_test;
    "pound_test" >:: pound_test;
    "remove_test" >:: remove_test;
]

end
