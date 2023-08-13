const fs = require('fs')
const path = require('path')
const readline = require('readline')

//при помощи модуля readline, создаем интерфейс построчного прочтения файлов

async function externalSort(filePath, chunkSize, outputFilePath) {
	const chanks = [] //массив для хранения путей
	let chunkCount = 0 // количество частей
	let currentChunk = [] // для хранения текущей части

	const rl = readline.createInterface({
		input: fs.createReadStream(filePath), //создаем поток для чтения файлов
		crlDelay: Infinity, //для правильной обработки строк
	})

	//для сортировки и записи данных следующая функция

	const sortAndSaveChunk = async (chunk, index) => {
		chunk.sort()
		const tmpFilePath = `${outputFilePath}.${index}` //путь к временному файлу
		await fs.promises.writeFile(tmpFilePath, chunk.join('\n')) // запись отсортированных данных в файл
		chunks.push(tmpFilePath) // добавление пути к временному файлу в массив
	}

	//создадим цикл для построчного чтения файла и создания временных файлов

	for await (const line of rl) {
		currentChunk.push(line) // добавление строки в текущий чанк

		// Если текущий размер чанка превышает указанный размер
		if (currentChunk.length >= chunkSize) {
			await sortAndSaveChunk(currentChunk, chunkCount) // сортировка и сохранение чанка
			currentChunk = [] // сброс текущего чанка
			chunkCount++ // увеличение счетчика чанков
		}
	}
	// если последний чанк не заполнен, то необходимо его обработать
	if (currentChunk.length > 0) {
		await sortAndSaveChunk(currentChunk, chunkCount) // сортировка и сохранение последнего чанка
		chunkCount++ // увеличение счетчика чанков
	}

	rl.close() //завершаем прочтение

	//создаем поток записи готового файла

	const outputStream = fs.createWriteStream(outputFilePath, { flags: 'a' })

	//объединяем отсортированные файлы в один

	for (const chunk of chunks) {
		const inputStream = fs.createReadStream(chunk, { encoding: 'utf-8' }) // поток чтения временного файла
		await new Promise((resolve) => {
			inputStream.pipe(outputStream, { end: false }) // запись данных из временного файла в итоговый файл
			inputStream.on('end', () => {
				fs.unlinkSync(chunk) // удаление временного файла после использования
				resolve()
			})
		})
	}
	outputStream.end() //завершение записи
}

const inputFilePath = 'text.txt'
const chunkSize = 1000000 // 1 миллион строк в каждом чанке
const outputFilePath = 'output.txt'

externalSort(inputFilePath, chunkSize, outputFilePath)
	.then(() => {
		console.log('Файл успешно отсортирован.')
	})
	.catch((err) => {
		console.error('Ошибка при сортировке файла:', err)
	})
