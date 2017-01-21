//----------------------------------------------
//------------------PageRANK-------------------
//----------------------------------------------

sc.setLogLevel("ERROR")
val path = "/home/poulain/Documents/Cours_DAC/BDLE/TP_GRAPH_1_2/"
val file = "facebook_edges.csv"
val lines = sc.textFile(path+file, 4)
val links = lines.map{ s => val parts = s.split(",")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
var ranks = links.mapValues(v => 1.0)
var iters = 10

for (i <- 2 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

val ranks5 = ranks

val output5 = ranks5.collect()

#Question_1
//--Question 1 --
//-- Exécuter ce programme. Classer le résultat obtenu ordre décroissant
//--de la valeur PageRank. Afficher les 10 premiers noeuds.

val test1=output5.sortBy(-_._2)
test1.take(10)


//--Question 2 --
//--Modifier le nombre d'itération en affectant à iters la valeur 10. Exécuter la boucle qui calcule le PageRank
//--puis récupérer le résultat dans une variable appelée output10.

var iters=10


for (i <- 2 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

val ranks10 = ranks

val output10 = ranks10.collect()


//--Question 3 --
Créer une variable raffiner qui pour chaque noeud retourne
la différence de score PageRank calculé avec 5 itérations et
celui calculé avec 10 itérations.
raffiner doit avoir le type (String, Double).

def subRowsArray(a:  Array[(String, Double)], b:  Array[(String, Double)]):  Array[(String, Double)] = {
val longueur=math.min(a.size,b.size)
val l: Array[(String, Double)] = new Array(longueur)
for (i <- 0 to longueur-1) {
  l(i)= (a(i)._1,math.abs(a(i)._2-b(i)._2))
  }
  return l
}

val raffiner = subRowsArray(output5,output10)

//--Question 4 --

val eps=0.01

sc.setLogLevel("ERROR")
val path = "/home/poulain/Documents/Cours_DAC/BDLE/TP_GRAPH_1_2/"
val file = "facebook_edges.csv"
val lines = sc.textFile(path+file, 4)
val links = lines.map{ s => val parts = s.split(",")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
var ranks = links.mapValues(v => 1.0)

val ranks_final=0
val ranks_k_1=0
var iters3=0
while(ranks_final>eps && iters3!=100) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      val ranks_k_1=ranks
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
      val ranks_final=(ranks.map({case(a,b)=>(b)-ranks_k_1.map({case(a,b)=>(b)})}))/size


      iters3=iters3+1
    }

iters2.take(1)
