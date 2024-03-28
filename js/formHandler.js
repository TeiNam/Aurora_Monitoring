async function loadInstanceList() {
    try {
        const response = await fetch('/api/instance_setup/list_instances/');
        if (!response.ok) {
            alert('Instance data를 불러오는 데 실패했습니다.');
            return;
        }
        const data = await response.json();

        if (!data.instances || !Array.isArray(data.instances)) {
            alert('Instance 데이터가 유효하지 않습니다.');
            return;
        }
        const tableBody = document.getElementById('instanceTable').getElementsByTagName('tbody')[0];
        tableBody.innerHTML = '';

        data.instances.forEach(instance => {
            if (!instance.cluster_name || !instance.instance_name) {
                console.error('Invalid instance data:', instance);
                return;
            }

            const row = tableBody.insertRow();
            row.insertCell().textContent = instance.environment;
            row.insertCell().textContent = instance.db_type;
            row.insertCell().textContent = instance.region;
            row.insertCell().textContent = instance.cluster_name;
            row.insertCell().textContent = instance.instance_name;
            row.insertCell().textContent = instance.host;
            row.insertCell().textContent = instance.port;

            const deleteCell = row.insertCell();
            const deleteButton = document.createElement('button');
            deleteButton.textContent = 'Delete';
            deleteButton.onclick = function() { deleteInstance(instance.instance_name); };
            deleteButton.className = 'smallerButton';
            deleteCell.appendChild(deleteButton);
        });
    } catch (error) {
        console.error('Error:', error);
        alert('Error loading instance list: ' + error.message);
    }
}

async function deleteInstance(instanceName) {
    if (!confirm('정말 삭제하시겠습니까?')) return;

    try {
        const response = await fetch(`/api/instance_setup/delete_instance/?instance_name=${instanceName}`, {
            method: 'DELETE'
        });
        if (!response.ok) {
            alert('Instance 삭제에 실패했습니다. 다시 시도해 주세요.');
            return;
        }
        const data = await response.json();
        alert(data.message);
        await loadInstanceList();
    } catch (error) {
        console.error('Error:', error);
        alert('Error: ' + error.message);
    }
}

document.getElementById('rdsForm').onsubmit = async (e) => {
    e.preventDefault();

    if (!document.getElementById('instance_name').value.trim()) {
        alert('Instance Name is required.');
        return;
    }
    if (!document.getElementById('host').value.trim()) {
        alert('Host is required.');
        return;
    }
    if (!document.getElementById('user').value.trim()) {
        alert('User is required.');
        return;
    }
    if (!document.getElementById('password').value.trim()) {
        alert('Password is required.');
        return;
    }

    const formData = new FormData(e.target);
    const formProps = Object.fromEntries(formData);

    try {
        const response = await fetch('/api/instance_setup/add_instance/', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(formProps)
        });
        if(response.ok) {
            alert('RDS Instance added successfully!');
            await loadInstanceList();
        } else {
            alert('Failed to add RDS Instance.');
        }
    } catch (error) {
        alert('Error: ' + error);
    }
};

window.onload = async function() {
    try {
        await loadInstanceList();
    } catch (error) {
        console.error('Error in loadInstanceList:', error);
        alert('Instance list를 불러오는 데 실패했습니다.');
    }
};

