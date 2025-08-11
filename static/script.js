import { parseM3U, categorizeItems } from './m3u_parser.js';

document.addEventListener('DOMContentLoaded', function () {
    const loadFromUrlTab = document.getElementById('load-from-url-tab');
    const loadFromFileTab = document.getElementById('load-from-file-tab');
    const duplicatesToolTab = document.getElementById('duplicates-tool-tab');

    const loadFromUrlContent = document.getElementById('load-from-url-content');
    const loadFromFileContent = document.getElementById('load-from-file-content');
    const duplicatesToolContent = document.getElementById('duplicates-tool-content');

    const loadFromXtreamBtn = document.getElementById('loadFromXtreamBtn');
    const uploadBtn = document.getElementById('uploadBtn');
    const listDuplicatesBtn = document.getElementById('listDuplicatesBtn');
    const deleteDuplicatesBtn = document.getElementById('deleteDuplicatesBtn');
    
    const resultsSection = document.getElementById('results-section');
    const statusMessages = document.getElementById('status-messages');

    // Sidebar navigation
    loadFromUrlTab.addEventListener('click', () => {
        showContent('url');
        updateActiveTab(loadFromUrlTab);
    });

    loadFromFileTab.addEventListener('click', () => {
        showContent('file');
        updateActiveTab(loadFromFileTab);
    });
    
    const viewAllContentTab = document.getElementById('view-all-content-tab');
    const viewAllContentContent = document.getElementById('view-all-content-content');

    duplicatesToolTab.addEventListener('click', () => {
        showContent('duplicates');
        updateActiveTab(duplicatesToolTab);
    });

    viewAllContentTab.addEventListener('click', () => {
        showContent('view-all');
        updateActiveTab(viewAllContentTab);
        fetchAllContent();
    });

    function showContent(type) {
        loadFromUrlContent.style.display = 'none';
        loadFromFileContent.style.display = 'none';
        duplicatesToolContent.style.display = 'none';
        viewAllContentContent.style.display = 'none';

        if (type === 'url') {
            loadFromUrlContent.style.display = 'block';
        } else if (type === 'file') {
            loadFromFileContent.style.display = 'block';
        } else if (type === 'duplicates') {
            duplicatesToolContent.style.display = 'block';
        } else if (type === 'view-all') {
            viewAllContentContent.style.display = 'block';
        }
    }
    
    function updateActiveTab(activeTab) {
        document.querySelectorAll('.sidebar .nav-link').forEach(link => {
            link.classList.remove('active');
        });
        activeTab.classList.add('active');
    }

    // Parse from Xtream
    loadFromXtreamBtn.addEventListener('click', async () => {
        const baseUrl = document.getElementById('xtreamBaseUrl').value;
        const username = document.getElementById('xtreamUsername').value;
        const password = document.getElementById('xtreamPassword').value;

        if (!baseUrl || !username || !password) {
            Swal.fire('Erro', 'Por favor, preencha todos os campos da Xtream.', 'error');
            return;
        }

        showLoading('Analisando a lista M3U da URL...');

        try {
            const response = await fetch('/api/parse_m3u', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ base_url: baseUrl, username: username, password: password }),
            });

            if (response.ok) {
                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let m3u_content = '';
                
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    m3u_content += decoder.decode(value, { stream: true });
                }

                const parsedItems = parseM3U(m3u_content);
                const categorized = categorizeItems(parsedItems);
                renderResults(categorized);
                Swal.fire('Sucesso', `Lista M3U analisada! Filmes: ${categorized.movies.length}, Séries: ${categorized.series.length}, Canais: ${categorized.channels.length}`, 'success');
            } else {
                const errorData = await response.json();
                throw new Error(errorData.error);
            }
        } catch (error) {
            Swal.fire('Erro', `Falha ao analisar a URL: ${error.message}`, 'error');
        } finally {
            hideLoading();
        }
    });

    // Upload from file
    uploadBtn.addEventListener('click', async () => {
        const fileInput = document.getElementById('fileInput');
        const files = fileInput.files;

        if (files.length === 0) {
            Swal.fire('Erro', 'Nenhum arquivo selecionado.', 'error');
            return;
        }

        showLoading('Enviando e processando arquivo...');

        const formData = new FormData();
        for (let file of files) {
            formData.append('m3uFile', file);
        }

        try {
            const uploadResponse = await fetch('/api/upload', {
                method: 'POST',
                body: formData,
            });

            const uploadResult = await uploadResponse.json();

            if (!uploadResponse.ok) {
                throw new Error(uploadResult.results.error[0].message);
            }
            
            const filenames = uploadResult.results.tempFiles;
            
            const processResponse = await fetch('/api/process_files', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ filenames: filenames })
            });
            
            const processResult = await processResponse.json();

            if (processResponse.ok) {
                Swal.fire('Sucesso', 'Arquivo M3U enviado e processado!', 'success');
                // Since process_files returns results for multiple files, we need to aggregate them.
                // For simplicity, we'll just display the results of the first file.
                const firstResult = processResult.results.processed_files[0];
                renderResults(firstResult);
            } else {
                throw new Error(processResult.results.error[0].message);
            }

        } catch (error) {
            Swal.fire('Erro', `Falha ao processar o arquivo: ${error.message}`, 'error');
        } finally {
            hideLoading();
        }
    });
    
    // Duplicates
    listDuplicatesBtn.addEventListener('click', async () => {
        showLoading('Listando duplicados...');
        try {
            const response = await fetch('/api/list_duplicates');
            const result = await response.json();
            const duplicatesResultDiv = document.getElementById('duplicatesResult');

            if (response.ok) {
                let html = '<p>Nenhum duplicado encontrado.</p>';
                if (result.results.length > 0) {
                    html = '<ul class="list-group">';
                    result.results.forEach(item => {
                        html += `<li class="list-group-item"><strong>${item.midia_titulo}</strong> (${item.midia_tipo}) - ${item.count} ocorrências</li>`;
                    });
                    html += '</ul>';
                }
                duplicatesResultDiv.innerHTML = html;
            } else {
                throw new Error(result.results.error[0].message);
            }
        } catch (error) {
            Swal.fire('Erro', `Falha ao listar duplicados: ${error.message}`, 'error');
        } finally {
            hideLoading();
        }
    });

    deleteDuplicatesBtn.addEventListener('click', async () => {
        showLoading('Removendo duplicados...');
        try {
            const response = await fetch('/api/delete_duplicates', { method: 'POST' });
            const result = await response.json();
            
            if (response.ok) {
                Swal.fire('Sucesso', `${result.results.deleted_count} duplicados removidos!`, 'success');
                document.getElementById('duplicatesResult').innerHTML = '';
            } else {
                throw new Error(result.results.error[0].message);
            }
        } catch (error) {
            Swal.fire('Erro', `Falha ao remover duplicados: ${error.message}`, 'error');
        } finally {
            hideLoading();
        }
    });


    let allCategorizedData = { movies: [], series: [], channels: [] };
    let currentPages = { movies: 0, series: 0, channels: 0 };
    const PAGE_SIZE = 99; // 33 rows of 3 cards

    function renderResults(categorizedData) {
        resultsSection.style.display = 'block';
        allCategorizedData = categorizedData;
        currentPages = { movies: 0, series: 0, channels: 0 };

        document.getElementById('filmes').innerHTML = '';
        document.getElementById('series').innerHTML = '';
        document.getElementById('canais').innerHTML = '';

        loadMore('filmes');
        loadMore('series');
        loadMore('canais');
    }
    
    function loadMore(tabId) {
        const items = allCategorizedData[tabId];
        const currentPage = currentPages[tabId];
        const container = document.getElementById(tabId);

        const start = currentPage * PAGE_SIZE;
        const end = start + PAGE_SIZE;
        const newItems = items.slice(start, end);

        newItems.forEach(item => {
            container.innerHTML += createItemCard(item);
        });

        currentPages[tabId]++;

        // Remove old button if it exists
        const oldButton = document.getElementById(`load-more-${tabId}`);
        if (oldButton) {
            oldButton.remove();
        }

        // Add new button if there are more items
        if (end < items.length) {
            const button = document.createElement('button');
            button.id = `load-more-${tabId}`;
            button.className = 'btn btn-outline-primary mt-3';
            button.textContent = 'Carregar Mais';
            button.addEventListener('click', () => loadMore(tabId));
            container.appendChild(button);
        }
    }

    // Event delegation for add buttons
    resultsSection.addEventListener('click', async (e) => {
        if (!e.target.classList.contains('add-item-btn')) return;

        const button = e.target;
        const itemJson = button.dataset.item;
        const item = JSON.parse(itemJson);

        showLoading('Adicionando ao banco de dados...');
        try {
            const response = await fetch('/api/add_items_to_db', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ items: [item] })
            });
            const result = await response.json();
            if (response.ok) {
                Swal.fire('Sucesso', 'Item adicionado ao banco de dados!', 'success');
                button.disabled = true;
                button.textContent = 'Adicionado';
            } else {
                throw new Error(result.error || 'Erro desconhecido');
            }
        } catch (error) {
            Swal.fire('Erro', `Falha ao adicionar item: ${error.message}`, 'error');
        } finally {
            hideLoading();
        }
    });

    function createItemCard(item) {
        const title = item.channel_name || item.tvg_name || 'Sem Título';
        const group = item.group_title || 'Sem Grupo';
        const logo = item.tvg_logo || 'https://via.placeholder.com/150';

        return `
            <div class="col-md-4">
                <div class="card mb-4 shadow-sm">
                    <img src="${logo}" class="card-img-top" alt="${title}" loading="lazy">
                    <div class="card-body">
                        <h5 class="card-title">${title}</h5>
                        <p class="card-text">${group}</p>
                        <button class="btn btn-primary btn-sm add-item-btn" data-item='${JSON.stringify(item)}'>
                            <i class="fas fa-plus"></i> Adicionar
                        </button>
                    </div>
                </div>
            </div>
        `;
    }

    function showLoading(message) {
        Swal.fire({
            title: 'Aguarde...',
            text: message,
            allowOutsideClick: false,
            didOpen: () => {
                Swal.showLoading();
            },
        });
    }

    function hideLoading() {
        Swal.close();
    }

    async function fetchAllContent() {
        showLoading('Carregando todo o conteúdo do banco de dados...');
        try {
            const response = await fetch('/api/get_all_content');
            const items = await response.json();
            renderAllContent(items);
        } catch (error) {
            Swal.fire('Erro', `Falha ao carregar conteúdo: ${error.message}`, 'error');
        } finally {
            hideLoading();
        }
    }

    function renderAllContent(items) {
        const container = document.getElementById('allContentContainer');
        if (items.length === 0) {
            container.innerHTML = '<p>Nenhum conteúdo encontrado no banco de dados.</p>';
            return;
        }

        let tableHtml = '<table class="table table-striped"><thead><tr><th>ID</th><th>Título</th><th>Tipo</th><th>Ações</th></tr></thead><tbody>';
        items.forEach(item => {
            tableHtml += `
                <tr>
                    <td>${item.midia_id}</td>
                    <td>${item.midia_titulo}</td>
                    <td>${item.midia_tipo}</td>
                    <td>
                        <button class="btn btn-sm btn-danger delete-content-btn" data-id="${item.midia_id}">Excluir</button>
                    </td>
                </tr>
            `;
        });
        tableHtml += '</tbody></table>';
        container.innerHTML = tableHtml;

        document.querySelectorAll('.delete-content-btn').forEach(button => {
            button.addEventListener('click', async (e) => {
                const mediaId = e.target.dataset.id;
                const result = await Swal.fire({
                    title: 'Confirmar Exclusão',
                    text: `Deseja realmente excluir o item ${mediaId}?`,
                    icon: 'warning',
                    showCancelButton: true,
                    confirmButtonText: 'Sim',
                    cancelButtonText: 'Não'
                });

                if (result.isConfirmed) {
                    showLoading('Excluindo item...');
                    try {
                        await fetch(`/api/delete_content/${mediaId}`, { method: 'DELETE' });
                        fetchAllContent(); // Refresh the list
                    } catch (error) {
                        Swal.fire('Erro', `Falha ao excluir item: ${error.message}`, 'error');
                    } finally {
                        hideLoading();
                    }
                }
            });
        });
    }
    
    document.getElementById('bulkDeleteMoviesBtn').addEventListener('click', () => bulkDelete('filme'));
    document.getElementById('bulkDeleteSeriesBtn').addEventListener('click', () => bulkDelete('serie'));

    async function bulkDelete(type) {
        const result = await Swal.fire({
            title: 'Confirmar Exclusão em Massa',
            text: `Deseja realmente excluir TODOS os itens do tipo '${type}'? Essa ação não pode ser desfeita.`,
            icon: 'warning',
            showCancelButton: true,
            confirmButtonText: 'Sim, excluir tudo',
            cancelButtonText: 'Não'
        });

        if (result.isConfirmed) {
            showLoading(`Excluindo todos os itens do tipo ${type}...`);
            try {
                const response = await fetch('/api/bulk_delete_content', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: type })
                });
                const data = await response.json();
                Swal.fire('Sucesso', `${data.deleted_count} itens do tipo ${type} foram excluídos.`, 'success');
                fetchAllContent(); // Refresh the list
            } catch (error) {
                Swal.fire('Erro', `Falha ao excluir em massa: ${error.message}`, 'error');
            } finally {
                hideLoading();
            }
        }
    }
});
