use std::fs::File;
use std::io::prelude::*;

#[derive(Default, Debug)]
struct TestCase {  // This is the test case struct that will be used to store all of our data. Each test case will have a name (the first line), and then the actual input string for each query in alphabetical order by variable name. The output strings are also stored here in alphabetical order by variable name. Note that this format assumes no whitespace between variables and their values or between them and their names (e.g., "x=1"). Also note that if your program can handle multiple queries on one line, you should still put all of those queries on a single line with spaces separating them as well as within their values (e.g., "x = 1 y = 2 z = 3") -- even though it looks like there might be an extra space after x=1 but before y=2 because Rust's lexer thinks they're separate words due to how we write main() below...
   
    pub var_names: Vec<String>,  //A vector containing the names of every variable/query in this testcase -- e.g., [a b c d] would mean that there are 4 variables named 'a', 'b', 'c' & 'd'.
   
    pub inputs: Vec<Vec<i32>>,   //A vector containing vectors representing each individual query's value for every variable/query given above -- e.g., [[4 5 6 7], [8 9 10 11]] means both tests cases have two different sets of inputs where q0 has input 1 4 5 6 7 ...and q1 has input 2 8 9 10 11 ...note these aren't necessarily ordered according to var_names! They're just separated into sub-vectors so I could use .iter().enumerate() later when printing out results nicely :) If you want more than 32 bits per int, simply add another dimension to this array!
	
    //TODO: what about outputs? We'll need some way of storing them too at some point...probably just make a new field called expected_outputs which stores exactly what we expect back from LSH-KV based off our inputs instead...? But maybe not since most programs don't care about any particular ordering / set size returned back? Maybe it doesn't matter unless you do something special like return only unique elements or something...but let me know if you need help figuring out whether your program actually cares about returning unique elements!! :D
}
// I'm using this instead of String::new() because Rust's lexer doesn't like me doing things like "let mut testcase = String::new();" anywhere other than within main(). It thinks that the semicolon is part of a function call, and as such it complains about an unexpected token/character if you put one after the closing parenthesis. So we just use let mut empty_str: Vec<char> = vec![] here to make sure there are no spaces in between any two characters (e.g., not between var and '='). Since new returns &CStrings, which will be deallocated by C when they go out-of-scope, we must ensure that our data isn't stored on stack space since it'll be freed up once get_next_testcase() finishes running...so instead we're returning a vector!! :D

#[allow(dead_code)]  // disable compiler warnings for unused variables -- useful if you want to try something wit
fn get_next_testcase(input: &mut File) -> TestCase {
    let mut linebuf: Vec<char>      = vec![]; // temporary storage for each individual word (a whitespace separated string or character) parsed from input file; NOTE THAT THIS ONLY WORKS WITH WHITESPACE SEPARATED WORDS!!! Try changing "while !linebuf.isEmpty() {" below [and maybe some other places] so they only check whether isspace(c as u32)? Also note that this won't work with unicode though..try looking into iterating through Unicode scalar values? Maybe also drop .toLowerCase() then too...? Eventually replace this whole thing with regexes probably...Or maybe just write another version of strtok?? ^^;;; ^^^^^^ nvm looks like rust has V8 style strings now thanks to UTF-16 support via char types so everything should work fine now ;) Just need to add std::collections - Rustdoc documentation v1.22 / nightly build 2017 09 20 according https://doc.rust-lang.org/nightly/std/collections/. At least until google releases their UTF8 library http://www0bloggercom/?u=http%3A%2F%2Fsrdjanstankovicorg&s=bloggersid&b=15421386261255461452 jk nobody cares about utf7x anymore anyway lolz
    let mut testcase: TestCase = Default::default();
    let mut var_name: Vec<char> = vec![];

     // Read first line containing var names
	if !input.bytes().filter(|b| *b == b'\n').next().is_some() {  // If there are more than 0 bytes left to read...
        while let Some(c) = readchar(&mut input) {  // ...read next char from file until encountering '/n', which indicates end-of-line has been reached so keep going until EOF too....or else this would never finish reading anything past 1st character in file :(
            match c as u8 {
                b'0'..= b'9', b',' => continue,   // Skip over numbers
             _   => break         // Otherwise assume pretty much any other ASCII symbol should stop us from continuing reading into current word/variable name/etc.; otherwise some unicode symbols might trigger false positives with delimiter matching later on!!!
            }
            
            } while let Some(c) = readchar(&mut input) {
// ...read next char from file until encountering '/n', which indicates end-of-line has been reached so keep going until EOF too....or else this would never finish reading anything past 1st character in file :(

    match c as u8 {
        b'0'..=b'9', b',' => continue,   // Skip over numbers
        _   => break         // Otherwise assume pretty much any other ASCII symbol should stop us from continuing reading into current word/variable name/etc.; otherwise some unicode symbols might trigger false positives with delimiter matching later on!!!
            }
        }
        while let Some(c) = readchar(&mut input) {
// ...read next char from file until encountering '/n', which indicates end-of-line has been reached so keep going until EOF too....or else this would never finish reading anything past 1st character in file :(
    match c as u8 {
        b'0'..=b'9', b',' => continue,   // Skip over numbers
        _   => break         // Otherwise assume pretty much any other ASCII symbol should stop us from continuing reading into current word/variable name/etc.; otherwise some unicode symbols might trigger false positives with delimiter matching later on!!!
            }
        if !input.bytes().filter(|v| *v == b';').next().is_some() && testcase.var_names[testcase.var_names-1] != ";" && /* we haven't just added another var name and */                                    !input      .bytes()                      .filter(|w| *w == b'"').next().is_some()     {         /* the current char is NOT part of some quoted string...*/                                          linebuf = vec!['"'] + &linebuf;              /* then add opening quote at start of list so query will know where it starts later on :)*/             while let Some(c) = readchar(&mut input){                  match c as u8{                     0..=127 => linebuf += [c],                         _       => panic!("Input contains non-printable characters!!!"),                   }}             continue;        }if c==''&&!name1,"x");}else{panic!("Expected alphabetical variable name but instead found nonalphabetical symbol: {}",expected);}}whileletSome&cc2::getcharsAndPushBackIntoVecs4ErrACLcodeExampleAuxFunc$RustMainFunctionsTODOwriteamainrsbuildtheallegroposetgrammaranduseitinAllegroCLallegroposisbuiltusingEinsteinDBTheGrammarBuildermainrswilltakeasanargumentatestcasesfilecontainingallofthetestsforagivenprogramintheformattestedaboveItllprobablybebettertofetchthisoverHTTPorHTTPSsomewheresincewehavetodownloadaprettybigfilenowanywaysThisshouldalsohelpwithautomatedtestingbyseedingourrandomnumbergeneratorwiththesevaluesinsteadofjustchoosingarandomseedforeachdaysoitsnotnecessarytoputthenamefieldherebutyoucouldifyouwantItllprobablyexpectsomethinglikeTestCasePROGRAMNAMEHerearesomeEncodingRulesThatArePrettyStandardMostlyselfexplanatoryexceptmaybeforquotedstringsthoughthatsupposedtobeUTF16andsometimespeopleuseunicodedespiteRustonlyacceptingASCIIliteralsrightnowIllseeifyouneedanythingmorecomplicatedthanthislateronthoughIfyoudontthenfeelfreeusetheexistingcodefromotherlanguageslikethoseonesbeloworsomethingelsewhichevermakesyourlifeeasierNooneislookingoveryourshoulderafterallFeelfreeaddcommentsoutsidefunctionsifyoureallywantbutIstronglysuggestdoingitinsideasmuchaspossibleBecausethisexampleismadefordifferentteamsworkingonlanguageinterfacesineverywaypossiblewerecommendthatwheneveracoderwriteshiscodeinitsserviceguidelineortutorialdocumentationinsidesomewordprocessorfirstbeforeportingittoideorgithubincasehisworkgetsconfusedwithsomeoneelseseveniftheresnoambiguityleftinthisfileaboutwhoseworkswhoAnyonecancompletelyrewritehispartswithoutaskingpermissiontooaslongastheykeepseniorcodersupdatedthroughconversationsinthelanguageappropriateforumPleasesubmitthesedocumentationfilesintoEinsteinDBSrcDatabasesDropboxfolderORanothersimilarplaceonlineSoeveryoneknowswhatsthedealEspeciallywhenpeoplestartuploadingnew 
         /* If the last element in var_names is not a semi-colon AND */
         c != ';' {  // ...and if you haven't reached the end of this line yet...

         linebuf.push(c)                                                   // Append this character to your current testcase string (assuming it's not already there)...

     } else if c == '=', input.bytes().filter(|v| *v == b';').next().is_some() {   // ...otherwise, stop reading once you reach either an equals sign or a semicolon, whichever comes first....

         let s: String = vec![linebuf].iter().collect();                    // Convert vector<char> into array of u8 bytes so that we can convert it into a &CString which will be automatically deallocated by C after going out-of-scope -- saves us from having to remember to manually free up any memory used for storing strings :D !!!!!!
         testcase.var_names[] = s;                                         // Read in variable name and add it to list of var names for current TestCase data structure..   eprintln!("{}", res);         println!("{:?}", res);     print!("{:?}\n\r", myvec2d);      dbg!(&myvec2d[0][]);    format!, "{:#?}"
//dbg macro shows type information whereas {:?}, {:#?}, etc don't show types          

     } else { break };  // Stop reading once you reach either an equals sign or ')' indicating end of our assignment statement (e.g., "a=(1, 2)" -> "(") where all characters past this point are irrelevant, at least as far as parsing TestCases goes anyway!!! :D :) ;) ;p XD xP X3 OXO ooxx ooOOoo ^u^ >w< <3 3u~ (*^ω&)(*U*) (>O>) (>◕ᴥ❤◕ฺ) (<ゝε・）ノ ﾃｩ━☆゜.*。☆Jk lol jajaja ノʘ‿ʘノ ☆⌒(*≧∀≦*)⌒☆ Σヽ(ﾟД´)ﾉ三彡Σヽ( ´Д` )人彡 ━╋━ ✖ ⊹ ❍ ✷ ψ ♞ ≛✮☪♬웃유♡๑ɷɷꂅ♥ஐ •̫͡•̫͡•̫ͻ 夢雨 ♯★〓¤°º¯°º±═Ϡ▓▓BIG BANGфячительность〓△Ðσřκ™ ßöšşėňčé и tímë™ ■ ‚‚ — ~—{{}}} {{~}} ° ¯ · ∙ ◦ ● ▲ ▶ ☺ ← ↑ → ↓ ⇨ ϟ ذ؝۵É۵»«::**=======¶¶ ¶¶==++++++ ¶¶ »»==========+++ ======== +++======†† ***""""" """"'";;;;;;; ;;___---____--__---___-----_____----_______---------________------__________---------,_______ _/ \ / \( -.- )/ `-' meowwwwwwww🙂😸😹😺㋏㋑㋒ংশৎॱ火業護士乄ّــعنايتبولس名孔大师怪小孩𖤍𝄞𝄢𝄪𝄭⁰¹²³⁴⁵⁶⁷
       else if c == ';'  

    // }

while let Some(c) = readchar(&mut input) {               
    match c as u8 {                             
        b'0'..=b'9', b',' => continue,          

        _               => break           
        if !input.bytes().filter(|v| *v == b';').next().is_some() && 
	    testcase.var_names[testcase.var_names - 1] != ";" {    
            linebuf.push(c);  

             else if c == ';'
            // }
            // else if c == ';' {
                //     let s: String = vec![linebuf].iter().collect();
                //     testcase.var_names[] = s;
                //     linebuf.clear();
                // }
                // else if c == ';' {
                    //     let s: String = vec![linebuf].iter().collect();
                    //     testcase.var_names[] = s;
                    //     linebuf.clear();
                    // }
                        
                    // }
                        
                        
                { {
                let s: String = vec![linebuf].iter().collect();                    
                testcase.var_names[] = s;       // Push the var name to the end of vector         

         } else { break };                        // If it is a new variable, push and stop reading this line     

         else if c == '=' || input.bytes().filter(|v| *v == b';').next().is_some() {}
        }                                       // If it is an equals sign, stop reading this line

        else if c == ';' { break };                // If it is a semicolon, stop reading this line

        else { break };                           // If it is anything else, stop reading this line
        while let Some(c) = input.bytes().filter(|v| *v == b';').next() {   
            match c as u8 {                                 
                b'0'..=b'9', b',' => continue,          
    
                _          => break               
            }                                           
    
             if !input.bytes().filter(|v| *v == b';').next().is_some() &&     testcase.var_names[testcase.var_names - 1] != ";" &&      c != ';' { 
    
                 linebuf[] = vec![linebuf].iter();                     // Push the var name to the end of vector         
    
             } else if c == '=' || input.bytes().filter(|v| *v == b';').next().is_some() {}  // If it is an assignment, read the next line      
             else if c == ';' {}                       
              // Else stop reading this variable and start a new one      
               else { break };                       
                // If it is a new variable, push and stop reading this line 
                    while let Some(_) = getchar(&mut stdin).unwrap(), let Ok(_) = getchar(&mut stdin), true{}
                        return TestCase::new();
                           fn main(){}

                           // Read first line containing var names
	for i in 0..testcase.var_names.len() {  

        while let Some(c) = readchar(&mut input) {                                          

            match c as u8 {  b'0'..=b'9', b',' => continue,    _               => break         }  

             if !input.bytes().filter(|v| *v == b';').next().is_some() &&  
             testcase.var_names[testcase.var_names - 1] != ";" 
             &&      c != ';' {} else if c == '=' || input.bytes().filter(|v| *v == b';').next().is_some() {}  // If it is an assignment, read the next line 
                  else if c == ';' {}                        // Else stop reading this variable and start a new one       else { break };                        // If it is a new variable, push and stop reading this line     while let Some(_) = getchar(&mut stdin).unwrap(), let Ok(_) = getchar(&mut stdin), true{}    return TestCase::new();   fn main(){}
         
         for j in 0..i + 1 { 

            while let Some(c) = readchar(&mut input){};  
                          match c as u8{};
                                             case:b'digit1-digit9', 
                                             comma=>continue;
                                             break;
                                            }else if !inputs&&||true{ 
                                                while let Some read char 
                                                continue
                                                break
                                                elseif==true||false&&==false{
                                                    {}
                                                }}
                                                elseif==true||false&&==false{
                                                    {}
                                                }  
                                        fn main( ){}        
                                         // Read first line containing var names
	for i in 0..testcase.var_names.len() {  

        while let Some(c) = readchar(&mut input) {                                          

            match c as u8 {  b'0'..=b'9', b',' => continue,    _               => break         }  

             if !input.bytes().filter(|v| *v == b';').next().is_some() &&     testcase.var_names[testcase.var_names - 1] != ";" &&      c != ';' {} else if c == '=' || input.bytes().filter(|v| *v == b';').next().is_some() {}  // If it is an assignment, read the next line      else if c == ';' {}                        // Else stop reading this variable and start a new one       else { break };                        // If it is a new variable, push and stop reading this line     while let Some(_) = getchar(&mut stdin).unwrap(), let Ok(_) = getchar(&mut stdin), true{}    return TestCase::new();   fn main(){}
         
         for j in 0..i + 1 { 

            while let Some(c) = readchar(&mut input){};                
            match c as u8{};                  
             case:b'digit1-digit9',
                comma=>continue;
                break;
            }else if !inputs && ||true{ while let Some readchar 
                continue
                break
                else if==true||false&&==false{{}}}}}fnmain(){
            
    
    let mut linebuf: Vec<char> = vec![]; 
    let mut testcase: TestCase = TestCase { ..Default::default() }; 

    // Read first line containing var names
    if let Some(_) = input.bytes().filter(|v| *v == b'\n').next() {  
        while let Some(c) = readchar(&mut input) {  
            match c as u8 {  b'0'..=b'9', b',' => continue,    _               => break         }  

             if !input.bytes().filter(|v| *v == b';').next().is_some() &&     testcase.var_names[testcase.var_names - 1] != ";" &&      c != ';' { 

                 linebuf[] = vec![linebuf].iter();                     // Push the var name to the end of vector         

             } else if c == ';' { 
                let s: String = vec![linebuf].iter().collect();                    
                testcase.var_names[] = s;       // Push the var name to the end of vector         

             } else { break };                        // If it is a new variable, push and stop reading this line     

             else if c == '=' || input.bytes().filter(|v| *v == b';').next().is_some() {}  // If it is an assignment, read the next line                            
        } 
    }

    while let Some(c) = input.bytes().filter(|v| *v == b';').next() {  
        match c as u8 {  
            b'0'..=b'9', b',' => continue,        _               => break         }  

         if !input.bytes().filter(|v| *v == b';').next().is_some() &&     testcase.var_names[testcase.var_names - 1] != ";" &&      c != ';' { 

             linebuf[] = vec![linebuf].iter();                     // Push the var name to the end of vector         

         } else if c == ';' { 
                let s: String = vec![linebuf].iter().collect();                    
                testcase.