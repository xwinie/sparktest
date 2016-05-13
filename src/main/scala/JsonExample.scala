
import org.json4s._
import org.json4s.jackson.JsonMethods._
object JsonExample extends App {
  val json = parse("""
         { "name": "joe",
           "children": [
             {
               "name": "Mary",
               "age": 5
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
                   """)

 println((json \ "children")(1))
  print(compact(render((json \ "children")(1))))

}