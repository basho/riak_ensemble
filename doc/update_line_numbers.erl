%% @doc Escript that will update the line numbers for links to functions
%% in the markdown documentation. Run like this:
%%
%% $ cd riak_ensemble/doc
%% $ escript update_line_numbers.erl ../ebin *.md

-module(update_line_numbers).
-mode(compile).
-export([main/1]).

main([Ebin | MdFiles]) ->
    FileContents = [begin
                        {ok, Text} = file:read_file(File),
                        {File, Text}
                    end || File <- MdFiles],
    WantedMods = lists:usort(lists:flatmap(fun parse_fun_mods/1, MdFiles)),
    AllLines = lists:flatten([get_line_nums(beam_filename(Ebin, Mod), Mod) || Mod <- WantedMods]),
    LineMap = dict:from_list(AllLines),
    [update_file(File, Text, LineMap) || {File, Text} <- FileContents],
    io:format("Updated function line numbers in ~p\n", [MdFiles]).

beam_filename(Ebin, Mod) ->
    filename:join(Ebin, atom_to_list(Mod) ++ ".beam").

update_file(Filename, Text, LineMap) ->
    NewText = update_lines(Text, LineMap),
    file:write_file(Filename, NewText).

parse_fun_mods(File) ->
    {ok, Text} = file:read_file(File),
    {ok, Re} = re:compile("\\[(\\w+):\\w+/\\d+\\]"),
    case re:run(Text, Re, [{capture, [1], binary}, global]) of
        nomatch ->
            [];
        {match, Matches} ->
            [binary_to_atom(M1, utf8) || [M1] <- lists:usort(Matches)]
    end.

mfa_bin(M, F, A) ->
    BM = atom_to_binary(M, utf8),
    BF = atom_to_binary(F, utf8),
    BA = integer_to_binary(A),
    <<"[", BM/binary, ":", BF/binary,"/", BA/binary, "]">>.

line_url(M, L) ->
    list_to_binary("../src/" ++ atom_to_list(M) ++ ".erl#L" ++ integer_to_list(L)).

get_line_nums(BeamFile, Mod) ->
    {ok, {_, [{abstract_code, {_, Items}}]}} = beam_lib:chunks(BeamFile, [abstract_code]),
    [{mfa_bin(Mod, Fun, Arity), line_url(Mod, Line)} || {function, Line, Fun, Arity, _} <- Items].

update_lines(Text, LineMap) ->
    Tokens = re:split(Text, "(\\[\\w+:\\w+/\\d+\\])\\s*(:.*|\\([^)]*\\))"),
    replace_line_nums(Tokens, LineMap, []).

replace_line_nums([], _, Acc) ->
    lists:reverse(Acc);
replace_line_nums([MaybeFun, Bin = <<Sep:8, _/binary>> | Rest], LineMap, Acc)
  when Sep =:= $:; Sep =:=$( ->
    case dict:find(MaybeFun, LineMap) of
        {ok, Line} ->
            LineText = case Sep of
                           $: ->
                               [": ", Line];
                           $( ->
                               ["(", Line, ")"]
                       end,
            NewText = list_to_binary([MaybeFun | LineText]),
            replace_line_nums(Rest, LineMap, [NewText|Acc]);
        _ ->
            replace_line_nums(Rest, LineMap, [Bin, MaybeFun | Acc])
    end;
replace_line_nums([Bin|Rest], LineMap, Acc) ->
    replace_line_nums(Rest, LineMap, [Bin|Acc]).

