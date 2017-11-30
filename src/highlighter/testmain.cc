#include <iostream>
#include "highlighter.h"

using namespace std;

int main() {
    // get a doc and a query terms
    Doc doc = "Hello world. This is Kan Wu. Today is rainy.";
    TermList query = {"Hello", "World"};
    // call highlighter    //TODO class?
    Snippet result = highlighter(doc, query);
    cout << "Result: " << result << endl;
    return 0;
}
