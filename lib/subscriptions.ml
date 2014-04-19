open Str
open OUnit

module Subscriptions : sig

    type 'a t

    val new_node : string -> 'a t

    val add_node : 'a t ->  string -> 'a -> 'a t

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

let tests = [
    "plus test" >:: plus_test;
    "pound_test" >:: pound_test;
]

end
