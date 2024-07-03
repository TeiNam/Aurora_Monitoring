let currentPage = 1;

async function fetchMemos(page = 1, page_size = 5) {
    try {
        const response = await fetch(`/api/memo/?page=${page}&page_size=${page_size}`);
        const data = await response.json();
        displayMemos(data.data);
        updatePaginationControls(page, data.total_pages);
    } catch (error) {
        console.error('Error fetching memos:', error);
    }
}

function updatePaginationControls(currentPage, totalPages) {
    const paginationDiv = document.getElementById('pagination');
    paginationDiv.innerHTML = '';

    if (currentPage > 1) {
        const prevButton = document.createElement('button');
        prevButton.textContent = 'Prev';
        prevButton.onclick = function() { fetchMemos(currentPage - 1); };
        paginationDiv.appendChild(prevButton);
    }

    let startPage = Math.max(currentPage - 2, 1);
    let endPage = Math.min(startPage + 4, totalPages);

    for (let page = startPage; page <= endPage; page++) {
        const pageButton = document.createElement('button');
        pageButton.textContent = page;
        pageButton.onclick = function() { fetchMemos(page); };
        if (page === currentPage) {
            pageButton.style.fontWeight = 'bold';
        }
        paginationDiv.appendChild(pageButton);
    }

    if (currentPage < totalPages) {
        const nextButton = document.createElement('button');
        nextButton.textContent = 'Next';
        nextButton.onclick = function() { fetchMemos(currentPage + 1); };
        paginationDiv.appendChild(nextButton);
    }
}

function displayMemos(memos) {
    const tableBody = document.getElementById('memoList').getElementsByTagName('tbody')[0];
    tableBody.innerHTML = '';

    memos.forEach(memo => {
        const row = tableBody.insertRow();

        const contentCell = row.insertCell();
        contentCell.className = 'memo-content-cell';
        contentCell.textContent = memo.content;

        // 메모 내용 셀을 더블 클릭했을 때 전체 텍스트 선택
        contentCell.addEventListener('dblclick', function() {
            window.getSelection().selectAllChildren(contentCell);
        });

        const deleteCell = row.insertCell();
        const deleteButton = document.createElement('button');
        deleteButton.textContent = 'Delete';
        deleteButton.className = 'delete-button';
        deleteButton.onclick = function() { deleteMemo(memo.id); };
        deleteCell.appendChild(deleteButton);
    });
}


async function addMemo() {
    const content = document.getElementById('memoContent').value;
    if (!content) {
        alert("Please enter some content for the memo.");
        return;
    }

    try {
        await fetch('/api/memo/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ content: content }),
        });
        fetchMemos(currentPage);  // Add 후 현재 페이지를 새로고침
    } catch (error) {
        console.error('Error adding memo:', error);
    }
}

async function deleteMemo(memoId) {
    try {
        await fetch(`/api/memo/${memoId}`, {
            method: 'DELETE',
        });
        fetchMemos(currentPage);  // Delete 후 현재 페이지를 새로고침
    } catch (error) {
        console.error('Error deleting memo:', error);
    }
}

document.addEventListener("DOMContentLoaded", () => fetchMemos(currentPage));
