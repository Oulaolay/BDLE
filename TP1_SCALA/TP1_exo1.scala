/*Manipulation de RDD Simple */
 val data=sc.textFile("/home/poulain/Documents/BDLE/data/wordcount.txt")
/*1) Extraire le 4e champ de chaque élément de data et le convertir en type double */
var q1=data.map(ligne=>ligne.split(" ")).map(tab=>tab(3).toDouble)
/*2) Construction d'une liste q1 contenant  */
var q2=q1.filter(x=>x>1000 && x<1300).map(x=>x.toInt)
/*3) Construire à partir de q2 une liste contenant les multiples de 3 et l’appeler q33. */
/* Faire de même pour les multiples de 4 et l’appeler q34. */
val q33=q2.filter(x=>x%3==0)
val q34=q2.filter(x=>x%4==0)
/*4)Construire une liste obtenue en divisant par 10 chaque élément de q33.*/
val q4=q33.map(x=>x/10)
/*5)Construire à partir de q4 un ensemble d’éléments (liste sans doublons).*/
var q5=q4.map(x=>x).distinct
/* Construire une liste de q2 qui sont multiple de 3 & 4*/
var q6=q33.intersection(q34)
/*6)Construire une liste de q2 qui sont mutilple de 3 mais pas de 4*/
var q7=q33.subtract(q34)
/* 7) Construire à partir de q2 une liste contenant les éléments de q2 multiples de 3 ou de 10*/
var q71=q2.filter(x=>x%10==0)
var q8=q33.union(q71)
/*8) q8 en double */
var q81=q8.map(x=>x.toDouble)
/*q9sum */
var q9=q81.map(x=>x).sum
var q91=q81.fold(0)(_+_)
var q92=q81.reduce((x,y)=>(x+y))
/*q9min*/
var q9min=q81.map(x=>x).min
/*q9max*/
var q9max=q81.map(x=>x).max
/*q9avg*/
var q91=q81.fold(0)(_+_)/q81.count
var q92=q81.reduce((x,y)=>(x+y))/q81.count
var q10=q2.map(x => q2.count(_ == x))
/*nombre d'occurence*/
var q10 = q2.map(x=> (x, 1)).reduceByKey(_ + _)
var q10 = q2.map(x=> (x, 1)).reduceByKey((x,y)=>x+y)


/* Manipulation*/
/*Structurer le contenu de data de sorte à obtenir une RDD de tableaux de chaînes de caractères.*/
var q1=data.map(ligne=>ligne.split(" ")).map(tab=>tab.toString)
var q13=q1.map(col=>col(3))
q13.take(100)
/*Transformer le contenu de list en une liste de paires (‘mot’, nb)
 mot correspond à la première colonne de list et nb sa troisième colonne.*/
val cle_val=data.map(x=>(x(0),x(2).toInt))
cle_val.take(10)
/*Grouper les paires par ‘mot’ et additionner leur nombre nb.*/
val cle_val_new=cle_val.reduceByKey((x,y)=>x+y)
