
//=====================================
// MOVIELENS : LES FILMS, USER, RATINGS
//======================================


val dataDir = "/home/poulain/Documents/BDLE/data/"

//----------
// LES USERS
//----------

case class Utilisateur(numU:Int, genre:String, age: Int, profession: Int, ville: String)

val USERS = sc.textFile(dataDir + "/users.dat", 10).
  map( ligne => ligne.split("::") ).
  map(t => Utilisateur(t(0).toInt, t(1), t(2).toInt, t(3).toInt, t(4))).toDS()

USERS.persist()
USERS.count

USERS.take(3).foreach(println)



//----------
// LES FILMS
//----------

case class Film(numF:Int, titre:String, genre: Array[String]) {
  override def toString() = "(" + numF + ", " + titre + ", " + afficheGenres() + ")"
  def afficheGenres() = "(" + genre.mkString(",") + ")"
}

def parseFilm(ligne: String): Film = {
  val c = ligne.split("::")
  val numF = c(0).toInt
  val titre = c(1)
  val genres = c(2).split("\\|")
  return Film(numF, titre, genres)
}

// lecture des films en chargment en mémoire
val FILMS = sc.textFile(dataDir + "/movies.dat", 10).
  map( ligne => parseFilm(ligne)).toDS()

FILMS.persist()
FILMS.count
FILMS.take(3)
FILMS.take(3).foreach(println)


//----------------------------------------
// LES AVIS des utilisateurs sur les films
//----------------------------------------

case class Avis(numU: Int, numF: Int, note: Int, date: Long)

val AVIS = sc.textFile(dataDir + "/ratings.dat", 10).coalesce(32).
  map( ligne => ligne.split("::") ).
  map(t => Avis(t(0).toInt, t(1).toInt, t(2).toInt, t(3).toLong)).toDS()

AVIS.persist()
AVIS.count
AVIS.take(3).foreach(println)




/*1) le nombre de notes (ratings) réalisées par chaque utilisateur identifié
par son UserID*/
val byUser = AVIS.groupBy("numU").count()
/*le nombre de notes (ratings) réalisées par chaque localisation donnée
par le Zip-code*/
val jointure=AVIS.join(USERS)
val byzip=jointure.groupBy("ville").count()
/*Le nombre de notes (ratings) réalisées par chaque groupe de genres de film.
/*top 10 users*/
val byUser = AVIS.groupBy("numU").count().sort($"count".desc)
byUser.take(10)
/*les films ayant reçu le moins de notes */
val jointure_F_A=AVIS.join(FILMS)
val byFilm=jointure_F_A.groupyBy("").
//val notes_final=AVIS.map(x=>(x(0),x(2)))
//.map(x=> (x, 1)).reduceByKey((x,y)=>x+y)
