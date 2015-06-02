# Babs
Esercizio sul data-set del Bay Area Bike Sharing

Si consideri il data-set messo a disposizione dal servizio di Bike Sharing dell’area di San Francisco (Bay Area).	 

Il data-set contiene, nella tabella “station”, le informazioni sui punti di interscambio installati, caratterizzato ciascuno da un nome, dalle coordinate (latitudine e longitudine), il numero di posti-bici disponibili (dockcount), e la città (landmark).

I singoli viaggi compiuti con le biciclette sono rappresentati nella tabella “trip”, che registra la durata (in secondi) di ciascun viaggio, unitamente alle stazioni di partenza ed arrivo, data e ora di presa e riporto, e l’identificativo numerico della bicicletta.

Si intende costruire un’applicazione JavaFX che permetta di interrogare tale base dati, e calcolare informazioni a proposito del funzionamento del servizio.

L’applicazione dovrà svolgere le seguenti funzioni:

1.	Permettere all’utente di selezionare il nome di due “station” da altrettanti menu a tendina (in ordine alfabetico) e visualizzare due numeri: il numero di “trip” tra tali stazioni, ed il tempo medio di percorrenza di tali trip.

  1. Risolvere l’esercizio utilizzando il campo trip.Duration

  1. Risolvere l’esercizio ipotizzando di non disporre del campo trip.Duration, e quindi calcolare la differenza tra trip.StartDate e trip.EndDate. Valutare se tale differenza debba essere calcolata a livello SQL oppure a livello Java.

2.	Permettere all’utente di selezionare, in aggiunta, una data, ed elencare tutti i trip, tra le due stazioni selezionate, che si svolgono in quel giorno.

  1.	Pre-impostare il calendario con la data del primo “trip” valido

  1.	Fare in modo che, se l’utente cerca di inserire una data esterna all’intervallo di date per il quale esistono dei “trip” validi, il calendario venga riportato comunque al valore estremo (il primo o l’ultimo giorno utile)

