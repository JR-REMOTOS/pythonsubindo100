let file;
let totalRequests = 0, adicionando = 0, existente = 0, Erro = 0, tempFile;

document.getElementById('fileInput').addEventListener('change', handleFile);
document.getElementById('loadButton').addEventListener('click', loadM3U);
document.getElementById('loadLargeButton').addEventListener('click', loadLargeM3U);

window.onload = loadM3UFiles;

function handleFile(event) {
    file = event.target.files[0];
    if (file) {
        document.querySelector('.custom-file-label').textContent = file.name;
    }
}

async function loadM3U() {
    if (!file) {
        Swal.fire('Erro', 'Nenhum arquivo carregado.', 'error');
        return;
    }

    const reader = new FileReader();
    reader.onload = async function(e) {
        const content = e.target.result;
        const data = new URLSearchParams({ m3u_content: content });

        try {
            const response = await fetch('/api/process_small', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: data
            });
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const result = await response.json();
            await processFileContent(result.results);
            updateProgressBar(100, file.size, file.size);
            Swal.fire('Concluído', 'Lista M3U carregada com sucesso!', 'success');
            loadM3UFiles();
        } catch (error) {
            console.error('Erro ao processar lista pequena:', error);
            Swal.fire('Erro', 'Erro ao processar a lista: ' + error.message, 'error');
        }
    };
    reader.readAsText(file);
}

async function loadLargeM3U() {
    if (!file) {
        Swal.fire('Erro', 'Nenhum arquivo carregado.', 'error');
        return;
    }

    const formData = new FormData();
    formData.append('m3uFile', file);

    Swal.fire({ title: 'Carregando...', text: 'Aguarde enquanto o arquivo é enviado.', allowOutsideClick: false, didOpen: () => Swal.showLoading() });

    try {
        const response = await fetch('/api/upload', { method: 'POST', body: formData });
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const result = await response.json();
        tempFile = result.results.tempFile;
        Swal.close();
        startLargeProcessing();
    } catch (error) {
        Swal.fire('Erro', 'Erro ao fazer upload: ' + error.message, 'error');
    }
}

async function startLargeProcessing() {
    Swal.fire({
        title: 'Processando...',
        html: 'Aguarde enquanto a lista é processada (<strong>0% concluído</strong>).',
        allowOutsideClick: false,
        didOpen: () => Swal.showLoading()
    });

    let completed = false;
    let lastProcessed = 0;

    while (!completed) {
        try {
            const response = await fetch('/api/process_large', {
                method: 'POST',
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                body: 'processLargeFile=' + encodeURIComponent(tempFile)
            });
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            const results = data.results;

            console.log('Resultados do backend:', results);  // Log para depuração

            if (results.error && results.error.length > 0) {
                Swal.fire('Erro', 'Erro ao processar: ' + (results.error[0].message || 'Erro desconhecido'), 'error');
                break;
            }

            updateProgress(results.progress, results.processed, results.total, results, false);
            Swal.getHtmlContainer().querySelector('strong').textContent = `${Math.round(results.progress)}% concluído (${results.processed}/${results.total} itens)`;

            if (results.processed === lastProcessed && results.progress < 100) {
                Swal.fire('Erro', 'O processamento parou de avançar. Verifique o servidor ou o banco de dados.', 'error');
                break;
            }
            lastProcessed = results.processed;

            if (results.progress >= 100) {
                completed = true;
                Swal.fire('Concluído', `Lista processada com sucesso! ${results.processed} itens processados.`, 'success');
                loadM3UFiles();
            }

            await new Promise(resolve => setTimeout(resolve, 500));
        } catch (error) {
            Swal.fire('Erro', 'Erro ao processar: ' + error.message, 'error');
            break;
        }
    }
}

async function processFileContent(data) {
    const tbody = document.getElementById('table-body');
    tbody.innerHTML = '';

    if (data.success && data.success.length > 0 || data.exists && data.exists.length > 0 || data.error && data.error.length > 0) {
        totalRequests = (data.success ? data.success.length : 0) + (data.exists ? data.exists.length : 0) + (data.error ? data.error.length : 0);
        let processed = 0;

        if (data.success) {
            data.success.forEach(item => {
                processed++;
                adicionando++;
                const row = `
                    <tr>
                        <td>Novo</td>
                        <td>${escapeString(item.type)}</td>
                        <td>${escapeString(item.data.titulo)}</td>
                        <td>${escapeString(item.data.groupTitle)}</td>
                        <td><a href="${item.data.url}" target="_blank">${escapeString(item.data.url.substring(0, 30))}...</a></td>
                    </tr>
                `;
                tbody.innerHTML += row;
                updateProgressBar((processed / totalRequests) * 100, file.size, file.size);
            });
        }
        if (data.exists) {
            data.exists.forEach(item => {
                processed++;
                existente++;
                const row = `
                    <tr>
                        <td>Duplicado</td>
                        <td>${escapeString(item.type)}</td>
                        <td>${escapeString(item.data.titulo)}</td>
                        <td>${escapeString(item.data.groupTitle)}</td>
                        <td><a href="${item.data.url}" target="_blank">${escapeString(item.data.url.substring(0, 30))}...</a></td>
                    </tr>
                `;
                tbody.innerHTML += row;
                updateProgressBar((processed / totalRequests) * 100, file.size, file.size);
            });
        }
        if (data.error) {
            data.error.forEach(item => {
                processed++;
                Erro++;
                const row = `
                    <tr>
                        <td>Erro: ${escapeString(item.message)}</td>
                        <td>${escapeString(item.type)}</td>
                        <td>${escapeString(item.data.titulo)}</td>
                        <td>${escapeString(item.data.groupTitle)}</td>
                        <td><a href="${item.data.url}" target="_blank">${escapeString(item.data.url.substring(0, 30))}...</a></td>
                    </tr>
                `;
                tbody.innerHTML += row;
                updateProgressBar((processed / totalRequests) * 100, file.size, file.size);
            });
        }
    } else {
        tbody.innerHTML = '<tr><td colspan="5">Nenhum item processado.</td></tr>';
    }

    updateStatus();
}

function updateProgress(progress, processed, total, results, showDetails = true) {
    const progressBar = document.getElementById('progressBar');
    const progressInMB = (processed * 1024 / (1024 * 1024)).toFixed(2);
    progressBar.style.width = `${progress}%`;
    progressBar.textContent = `${Math.round(progress)}% (${progressInMB} MB)`;

    totalRequests = total;
    adicionando = results.success ? results.success.length : 0;
    existente = results.exists ? results.exists.length : 0;
    Erro = results.error ? results.error.length : 0;

    if (showDetails) {
        const tbody = document.getElementById('table-body');
        tbody.innerHTML = '';

        if (results.success && results.success.length > 0) {
            results.success.forEach(item => {
                const row = `
                    <tr>
                        <td>Sucesso</td>
                        <td>${escapeString(item.type)}</td>
                        <td>${escapeString(item.data.titulo)}</td>
                        <td>${escapeString(item.data.groupTitle)}</td>
                        <td><a href="${item.data.url}" target="_blank">${escapeString(item.data.url.substring(0, 30))}...</a></td>
                    </tr>
                `;
                tbody.innerHTML += row;
            });
        }
        if (results.exists && results.exists.length > 0) {
            results.exists.forEach(item => {
                const row = `
                    <tr>
                        <td>Duplicado</td>
                        <td>${escapeString(item.type)}</td>
                        <td>${escapeString(item.data.titulo)}</td>
                        <td>${escapeString(item.data.groupTitle)}</td>
                        <td><a href="${item.data.url}" target="_blank">${escapeString(item.data.url.substring(0, 30))}...</a></td>
                    </tr>
                `;
                tbody.innerHTML += row;
            });
        }
        if (results.error && results.error.length > 0) {
            results.error.forEach(item => {
                const row = `
                    <tr>
                        <td>Erro: ${escapeString(item.message)}</td>
                        <td>${escapeString(item.type)}</td>
                        <td>${escapeString(item.data.titulo)}</td>
                        <td>${escapeString(item.data.groupTitle)}</td>
                        <td><a href="${item.data.url}" target="_blank">${escapeString(item.data.url.substring(0, 30))}...</a></td>
                    </tr>
                `;
                tbody.innerHTML += row;
            });
        }
    }

    updateStatus();
}

async function loadM3UFiles() {
    try {
        const response = await fetch('/api/list_files', {
            method: 'GET',
            headers: { 'Accept': 'application/json' }
        });
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const result = await response.json();
        const files = result.results;

        const tbody = document.getElementById('m3u-files-body');
        tbody.innerHTML = '';

        files.forEach(file => {
            const row = `
                <tr ${file.status === 'Incompleto' ? 'class="incomplete"' : ''}>
                    <td>${escapeString(file.name)}</td>
                    <td>${file.total}</td>
                    <td>${file.processed}</td>
                    <td>${file.status}</td>
                    <td>
                        ${file.status === 'Incompleto' ? '<button class="btn btn-sm btn-warning reprocess-file" data-file-name="' + file.name + '">Reenviar</button>' : ''}
                        <button class="btn btn-sm btn-danger delete-file" data-file-name="${file.name}">Excluir</button>
                    </td>
                </tr>
            `;
            tbody.innerHTML += row;
        });

        document.querySelectorAll('.reprocess-file').forEach(button => {
            button.addEventListener('click', function() {
                const fileName = this.getAttribute('data-file-name');
                reprocessFile(fileName);
            });
        });

        document.querySelectorAll('.delete-file').forEach(button => {
            button.addEventListener('click', function() {
                const fileName = this.getAttribute('data-file-name');
                deleteFile(fileName);
            });
        });
    } catch (error) {
        console.error('Erro ao carregar listas M3U:', error);
        Swal.fire('Erro', 'Não foi possível carregar as listas M3U salvas.', 'error');
    }
}

async function reprocessFile(fileName) {
    Swal.fire({
        title: 'Reprocessando...',
        text: `Reprocessando a lista ${fileName}.`,
        allowOutsideClick: false,
        didOpen: () => Swal.showLoading()
    });

    try {
        const response = await fetch('/api/reprocess', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: 'reprocessFile=' + encodeURIComponent(fileName)
        });
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const result = await response.json();
        const results = result.results;

        if (results.error && results.error.length > 0) {
            Swal.fire('Erro', 'Erro ao reprocessar: ' + results.error[0].message, 'error');
        } else {
            updateProgress(results.progress, results.processed, results.total, results, false);
            Swal.fire('Concluído', `Lista ${fileName} reprocessada com sucesso! ${results.processed} itens processados.`, 'success');
            loadM3UFiles();
        }
    } catch (error) {
        Swal.fire('Erro', 'Erro ao reprocessar: ' + error.message, 'error');
    }
}

async function deleteFile(fileName) {
    Swal.fire({
        title: 'Confirmar Exclusão',
        text: `Tem certeza que deseja excluir a lista ${fileName} e todos os dados associados do banco?`,
        icon: 'warning',
        showCancelButton: true,
        confirmButtonText: 'Sim, excluir',
        cancelButtonText: 'Não'
    }).then(async (result) => {
        if (result.isConfirmed) {
            try {
                const response = await fetch('/api/delete', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: 'deleteFile=' + encodeURIComponent(fileName)
                });
                if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                const result = await response.json();
                if (result.results.success) {
                    Swal.fire('Excluído', `Lista ${fileName} excluída com sucesso!`, 'success');
                    loadM3UFiles();
                } else {
                    Swal.fire('Erro', 'Erro ao excluir: ' + (result.results.error[0].message || 'Erro desconhecido'), 'error');
                }
            } catch (error) {
                Swal.fire('Erro', 'Erro ao excluir: ' + error.message, 'error');
            }
        }
    });
}

function escapeString(str) {
    if (!str) return '';
    return String(str)
        .replace(/[\r\n\t]+/g, ' ')
        .replace(/'/g, "\\'")
        .replace(/"/g, '\\"')
        .replace(/</g, '<')
        .replace(/>/g, '>')
        .replace(/&/g, '&')
        .replace(/\s+/g, ' ')
        .trim();
}

function updateProgressBar(progress, fileSize, offset) {
    const progressBar = document.getElementById('progressBar');
    const progressInMB = (offset / (1024 * 1024)).toFixed(2);
    progressBar.style.width = `${progress}%`;
    progressBar.textContent = `${Math.round(progress)}% (${progressInMB} MB)`;
}

function updateStatus() {
    document.getElementById('totalRequests').textContent = totalRequests;
    document.getElementById('adicionando').textContent = adicionando;
    document.getElementById('existente').textContent = existente;
    document.getElementById('Erro').textContent = Erro;
}

document.querySelector('#fileInput').addEventListener('change', function(event) {
    const fileName = event.target.files[0] ? event.target.files[0].name : 'Nenhum arquivo selecionado';
    document.querySelector('.custom-file-label').textContent = fileName;
});