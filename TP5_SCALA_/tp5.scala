
//=====================================
// MOVIELENS : LES FILMS, USER, RATINGS
//======================================


val dataDir = "/home/poulain/Documents/Cours_DAC/BDLE/TP5_scala"

//----------
// LES USERS
//----------

case class Utilisateur(numU:Int, genre:String, age: Int, profession: Int, ville: String)

val USERS = sc.textFile(dataDir+"/users.dat", 10).map( ligne => ligne.split("::") ).map(t => Utilisateur(t(0).toInt, t(1), t(2).toInt, t(3).toInt, t(4))).toDS()

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

//----------------------------------------
// Exercice1 : Equi-jointure parallèle et repartie:
//----------------------------------------

        //----------------------------------------
        // 2016: RDD
        //----------------------------------------
          val jointure=AVIS.join(USERS,"numU")
          jointure.count

          // taille du shuffle read = 11,5KB + 382KB =>

          //----------------------------------------
          // 2015:
          //----------------------------------------

          //CREATION DES USERS RDD
            val USERSRDD = sc.textFile("/home/poulain/Documents/Cours_DAC/BDLE/TP5_scala/users.dat", 10).map( ligne => ligne.split("::") ).map(t => (t(0).toInt, (t(1), t(2).toInt, t(3), t(4))))
          // nbre d'utilisateurs
            USERSRDD.count

            val RATINGSRDD = sc.textFile("/home/poulain/Documents/Cours_DAC/BDLE/TP5_scala/ratings.dat", 20).map( ligne => ligne.split("::") ).map(t => (t(0).toInt, (t(1), t(2), t(3))))
            // nbre de notes
             RATINGSRDD.count

             USERSRDD.partitions.size
             RATINGSRDD.partitions.size


             val j = USERSRDD.join(RATINGSRDD,  5)
             j.count

             // taille du shuffle write = 12.8 MB (12,6 + 117.1)
             - pour le stage de mapping sur users.dat (Stage 12)
             Shuffle Write size pour chaque tasks : 12kb (18ms)
             - pour le stage de mapping sur ratings.dat (Stage 12)
             Shuffle Write size pour chaque tasks : 650kb (0.3s)
 -            pour la jointure : 5 paquet de 2,5 Mo. (1s)




 //----------------------------------------
 // Exercice 2 : Equi-jointure parallèle entre des données déjà fragmentées:
 //----------------------------------------

    //----------------------------------------
    // 2016: RDD
    //----------------------------------------

    import org.apache.spark.HashPartitioner
   val U = USERSRDD.partitionBy(new HashPartitioner(5))
   U.setName("Users").persist()
   U.count

   val R = RATINGSRDD.partitionBy(new HashPartitioner(5))
   R.setName("Ratings").persist()
   R.count

   R.lookup(1234).take(10)

   RATINGSRDD.lookup(1234).take(10)
   20taches nécessaires pour ce look up.

   val j2 = R.join(U)
   j2.count
   // 5 taches pour la jointure
    Un seul stage suffit car...

//----------------------------------------
// Exercice 3 :E qui-jointure par broadcast join
//----------------------------------------








//----------------------------------------
// Exercice 5 : Produit Cartesien
//----------------------------------------

//Le but de cet exercice est de calculer la similarité entre 2 utilisateurs de Movie Lens
val avis = sc.textFile(dataDir + "/ratings.dat").map(x=>x.split("::")).map(x=>(x(0).toInt,x(1).toInt))

val avis_user = avis.groupByKey()

//Fonction qui calcule la similarité entre 2 listes de films : sim(x,y)= cardinal de l'intersection/cardinal de l'union

def simil(l1:Iterable[Int],l2:Iterable[Int]):Double =
{
 val list1 = l1.toList
 val list2 = l2.toList
 val inters = list1.intersect(list2).size.toDouble
 val uni = (list1.size + list2.size - inters).toDouble
 inters/uni
}

val C = avis_user.cartesian(avis_user).map{case((u1,l1),(u2,l2))=>(u1,u2,simil(l1,l2))}

















/*1) le nombre de notes (ratings) réalisées par chaque utilisateur identifié
par son UserID*/
val byUser = AVIS.groupBy("numU").count()
/*le nombre de notes (ratings) réalisées par chaque localisation donnée
par le Zip-code*/
val jointure=AVIS.join(USERS,"numU")
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
